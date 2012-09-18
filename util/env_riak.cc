// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// env_posix.cc brutally adjusted for riak

#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/file.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env_riak.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/posix_logger.h"
#include "db/dbformat.h"

#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
#define HAVE_FADVISE
#endif

namespace leveldb {

//
// tiered locks:  a technique to prioritize compaction writes
//                across simultaneous database instances

// Hold ReadLock while writing imm to Level-0,
//  poll WriteLock if higher other compaction level
pthread_rwlock_t gThreadLock0;

// Hold ReadLock while writing Level-0 to Level-1,
//  poll WriteLock if higher other compaction level
pthread_rwlock_t gThreadLock1;


namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

// background routines to close and/or unmap files
static void BGFileCloser(void* file_info);
static void BGFileCloser2(void* file_info);
static void BGFileUnmapper(void* file_info);
static void BGFileUnmapper2(void* file_info);

// data needed by background routines for close/unmap
struct BGCloseInfo
{
    int fd_;            //!< file handle, assumed open
    void * base_;       //!< address of memory mapping
    size_t offset_;     //!< offset within the file where mapping starts
    size_t length_;     //!< length of file mapped
    size_t unused_;     //!< ending portion of the mapping/file that is unused

    BGCloseInfo(int fd, void * base, size_t offset, size_t length, size_t unused)
        : fd_(fd), base_(base), offset_(offset), length_(length), unused_(unused) {};
};


// used to read recovery logs and within ReadFileToString()
class RiakSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  RiakSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~RiakSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};


// pread() based random-access
class RiakRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  RiakRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~RiakRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};


// mmap() based random-access
class RiakMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  int fd_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  RiakMmapReadableFile(const std::string& fname, void* base, size_t length, int fd)
      : filename_(fname), mmapped_region_(base), length_(length), fd_(fd)
  {
#if defined(HAVE_FADVISE)
    posix_fadvise(fd_, 0, length_, POSIX_FADV_RANDOM);
#endif
  }
  virtual ~RiakMmapReadableFile()
  {
    BGCloseInfo * ptr=new BGCloseInfo(fd_, mmapped_region_, 0, length_, 0);
    Env::Default()->Schedule(&BGFileCloser, ptr, 4);
  };

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};


// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class RiakMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file

  // Have we done an munmap of unsynced data?
  bool pending_sync_;

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion(bool and_close=false) {
    bool result = true;
    if (base_ != NULL) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }

      BGCloseInfo * ptr=new BGCloseInfo(fd_, base_, file_offset_, limit_-base_, limit_-dst_);
      if (and_close)
      {
          // do this in foreground unfortunately, bug where file not
          //  closed fast enough for reopen
          BGFileCloser2(ptr);
          fd_=-1;
      }   // if
      else
      {
          RiakDefaultEnv()->Schedule(&BGFileUnmapper2, ptr, 4);
      }   // else

      file_offset_ += limit_ - base_;
      base_ = NULL;
      limit_ = NULL;
      last_sync_ = NULL;
      dst_ = NULL;

      // Increase the amount we map the next time, but capped at 1MB
      if (map_size_ < (1<<20)) {
        map_size_ *= 2;
      }
    }
    else if (and_close && -1!=fd_)
    {
        close(fd_);
        fd_=-1;
    }   // else if

    return result;
  }

  bool MapNewRegion() {
    size_t offset_adjust;

    // append mode file might not have file_offset_ on a page boundry
    offset_adjust=file_offset_ % page_size_;
    if (0!=offset_adjust)
        file_offset_-=offset_adjust;

    assert(base_ == NULL);
    if (ftruncate(fd_, file_offset_ + map_size_) < 0) {
      return false;
    }
    void* ptr = mmap(NULL, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd_, file_offset_);
    if (ptr == MAP_FAILED) {
      return false;
    }
    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_ + offset_adjust;
    last_sync_ = base_;
    return true;
  }

 public:
  RiakMmapFile(const std::string& fname, int fd,
                size_t page_size, size_t file_offset=0L)
      : filename_(fname),
        fd_(fd),
        page_size_(page_size),
        map_size_(Roundup(20 * 1048576, page_size)),
        base_(NULL),
        limit_(NULL),
        dst_(NULL),
        last_sync_(NULL),
        file_offset_(file_offset),
        pending_sync_(false) {
    assert((page_size & (page_size - 1)) == 0);
  }


  ~RiakMmapFile() {
    if (fd_ >= 0) {
      RiakMmapFile::Close();
    }
  }

  virtual Status Append(const Slice& data) {
    const char* src = data.data();
    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (!UnmapCurrentRegion() ||
            !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }
    return Status::OK();
  }

  virtual Status Close() {
    Status s;

    if (!UnmapCurrentRegion(true)) {
        s = IOError(filename_, errno);
    }

    fd_ = -1;
    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() {
    return Status::OK();
  }

  virtual Status Sync() {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fdatasync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }

    return s;
  }
};


// matthewv July 17, 2012 ... riak was overlapping activity on the
//  same database directory due to the incorrect assumption that the
//  code below worked within the riak process space.  The fix leads to a choice:
// fcntl() only locks against external processes, not multiple locks from
//  same process.  But it has worked great with NFS forever
// flock() locks against both external processes and multiple locks from
//  same process.  It does not with NFS until Linux 2.6.12 ... other OS may vary.
//  SmartOS/Solaris do not appear to support flock() though there is a man page.
// Pick the fcntl() or flock() below as appropriate for your environment / needs.

static int LockOrUnlock(int fd, bool lock) {
#ifndef LOCK_UN
    // works with NFS, but fails if same process attempts second access to
    //  db, i.e. makes second DB object to same directory
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
#else
  // does NOT work with NFS, but DOES work within same process
  return flock(fd, (lock ? LOCK_EX : LOCK_UN) | LOCK_NB);
#endif
}

class RiakFileLock : public FileLock {
 public:
  int fd_;
};


class RiakEnv : public Env {
 public:
  RiakEnv();
  virtual ~RiakEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    exit(1);
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new RiakSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
#if 0
      // going to let page cache tune the file
      //  system reads instead of hoping to better
      //  manage through memory mapped files.
    } else if (sizeof(void*) >= 8) {
      // Use mmap when virtual address-space is plentiful.
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
            *result = new RiakMmapReadableFile(fname, base, size, fd);
        } else {
          s = IOError(fname, errno);
          close(fd);
        }
      }
#endif
    } else {
      *result = new RiakRandomAccessFile(fname, fd);
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) {
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new RiakMmapFile(fname, fd, page_size_);
    }
    return s;
  }

  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result) {
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
      *result = NULL;
      s = IOError(fname, errno);
    } else
    {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok())
      {
          *result = new RiakMmapFile(fname, fd, page_size_, size);
      }   // if
      else
      {
          s = IOError(fname, errno);
          close(fd);
      }   // else
    }   // else
    return s;
  }



  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  };

  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      RiakFileLock* my_lock = new RiakFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    RiakFileLock* my_lock = reinterpret_cast<RiakFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg, int state=0, bool imm_flag=false);

  virtual void StartThread(void (*function)(void* arg), void* arg);

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &RiakEnv::gettid);
      return Status::OK();
    }
  }

  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    struct timespec ts;
    int ret_val;

    if (0!=micros)
    {
        micros=(micros/clock_res_ +1)*clock_res_;
        ts.tv_sec=micros/1000000;
        ts.tv_nsec=(micros - ts.tv_sec) *1000;

        do
        {
#if _POSIX_TIMERS >= 200801L
            // later ... add test for CLOCK_MONOTONIC_RAW where supported (better)
            ret_val=clock_nanosleep(CLOCK_MONOTONIC,0, &ts, &ts);
#else
            ret_val=nanosleep(&ts, &ts);
#endif
        } while(EINTR==ret_val && 0!=(ts.tv_sec+ts.tv_nsec));
    }   // if
  }  // SleepForMicroSeconds


  virtual int GetBackgroundBacklog() const {return(bg_backlog_);};


 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0)
    {
        // later, this could start a "throw"
        syslog(LOG_WARNING, "pthread %s: %s", label, strerror(result));
    }
  }

  void StartBackgroundThreads();

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<RiakEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  port::Mutex mu_;
  port::CondVar bgsignal_;
  pthread_t bgthread_compact_;  //!< normal compactions
  pthread_t bgthread_level0_;   //!< level 0 to level 1 compactions
  pthread_t bgthread_files_;    //!< background unmap / close
  pthread_t bgthread_imm_;      //!< imm_ to level 0 compactions
  bool started_bgthread_;
  volatile int bg_backlog_;// count of items on 3 compaction queues
  int64_t clock_res_;

  // Entry per Schedule() call
  struct BGItem
  {
      void* arg;
      void (*function)(void*);

      BGItem(void* Arg, void (*Function)(void*))
          : arg(Arg), function(Function) {};
  };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_compact_;     //normal compactions
  BGQueue queue_level0_;    // level 0 to level 1 compactions
  BGQueue queue3_files_;    //background unmap / close
  BGQueue queue_imm_;    // imm_ to level 0 compactions

};

RiakEnv::RiakEnv() : page_size_(getpagesize()),
                     bgsignal_(&mu_),
                     started_bgthread_(false),
                     bg_backlog_(0),
                     clock_res_(1)
{
    struct timespec ts;

#if _POSIX_TIMERS >= 200801L
    clock_getres(CLOCK_MONOTONIC, &ts);
    clock_res_=ts.tv_sec*1000000+ts.tv_nsec/1000;
    if (0==clock_res_)
        ++clock_res_;
#endif

}

// state: 0 - legacy/default, 4 - close/unmap, 2 - test for queue switch, 1 - imm/high priority
void
RiakEnv::Schedule(
    void (*function)(void*),
    void* arg,
    int state,
    bool imm_flag)
{
    port::MutexLock lock(mu_);
    BGQueue::iterator it;
    bool found;
    BGItem new_item(arg, function);
    BGQueue * queue_ptr;

    // Start background thread if necessary
    if (!started_bgthread_)
    {
        started_bgthread_ = true;

        StartBackgroundThreads();
    }   // if

    // If the queue is currently empty, the background thread may currently be
    // waiting.
    if (queue_compact_.empty() || queue_level0_.empty() || queue3_files_.empty() || queue_imm_.empty()) {
        bgsignal_.SignalAll();
    }

    switch(state)
    {
        // low priority compaction (not imm, not level0)
        case 0:
            queue_compact_.push_back(new_item);
            break;

        // close / unmap memory mapped files
        case 4:
            queue3_files_.push_back(new_item);
            break;

        // state 1, adding imm_ or level 0 (nothing already scheduled)
        case 1:
            if (imm_flag)
                queue_imm_.push_back(new_item);
            else
                queue_level0_.push_back(new_item);
            break;


        // potentially switch priority queues
        //   (only "switch" lists if found waiting on a lower priority compaction list)
        case 2:
            if (imm_flag)
                queue_ptr=&queue_imm_;
            else
                queue_ptr=&queue_level0_;

            // search slowest queue
            for (found=false, it=queue_compact_.begin(); queue_compact_.end()!=it && !found; ++it)
            {
                found= (it->arg == arg);
                if (found)
                {
                    BGQueue::iterator del_it;

                    del_it=it;
                    ++it;

                    queue_compact_.erase(del_it);
                    queue_ptr->push_back(new_item);
                }   // if
            }   // for

            if (!found && imm_flag)
            {
                // search level 0 queue
                for (found=false, it=queue_level0_.begin(); queue_level0_.end()!=it && !found; ++it)
                {
                    found= (it->arg == arg);
                    if (found)
                    {
                        BGQueue::iterator del_it;

                        del_it=it;
                        ++it;

                        queue_level0_.erase(del_it);
                        queue_imm_.push_back(new_item);
                    }   // if
                }   // for
            }   // if
            break;
    }   // switch

    bg_backlog_=queue_imm_.size()+queue_level0_.size()*2+queue_compact_.size()*2;

    return;
}


void
RiakEnv::StartBackgroundThreads()
{
    // future, set SCHED_FIFO ... assume application at priority 50
    /// legacy compaction thread priority 45, 5 points lower
    PthreadCall(
        "create thread compact",
        pthread_create(&bgthread_compact_, NULL,  &RiakEnv::BGThreadWrapper, this));

    /// level0 compaction, speed thread priority 60
    PthreadCall(
        "create thread level0",
        pthread_create(&bgthread_level0_, NULL,  &RiakEnv::BGThreadWrapper, this));

    /// close / unmap thread priority 47, higher than compaction but lower than application
    PthreadCall(
        "create thread files",
        pthread_create(&bgthread_files_, NULL,  &RiakEnv::BGThreadWrapper, this));

    /// imm->level0 dump
    PthreadCall(
        "create thread imm",
        pthread_create(&bgthread_imm_, NULL,  &RiakEnv::BGThreadWrapper, this));

    return;

}   // RiakEnv::StartBackgroundThreads


void RiakEnv::BGThread()
{
    BGQueue * queue_ptr(NULL);
    void (*function)(void*);
    void * arg;

    // pick source of thread's work
    if (pthread_self()==bgthread_imm_)
        queue_ptr=&queue_imm_;

    else if (pthread_self()==bgthread_files_)
        queue_ptr=&queue3_files_;

    else if (pthread_self()==bgthread_level0_)
        queue_ptr=&queue_level0_;

    else if (pthread_self()==bgthread_compact_)
             queue_ptr=&queue_compact_;


    while (true)
    {
        // Wait until there is an item that is ready to run
        {
            port::MutexLock lock(mu_);

            while (queue_ptr->empty())
            {
                bgsignal_.Wait();
            }   // while

            function = queue_ptr->front().function;
            arg = queue_ptr->front().arg;
            queue_ptr->pop_front();

            bg_backlog_=queue_imm_.size()+queue_level0_.size()+queue_compact_.size();
        }   // end MutexLock scope

        (*function)(arg);

    }   // while(true)
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}

static void* StartThreadWrapper(void* arg) {
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void RiakEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}


// this was a reference file:  unmap, purge page cache, close
void BGFileCloser(void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_DONTNEED);
#endif

    if (0 != file_ptr->unused_)
        ftruncate(file_ptr->fd_, file_ptr->offset_ + file_ptr->length_ - file_ptr->unused_);

    close(file_ptr->fd_);
    delete file_ptr;

    return;

}   // BGFileCloser


// this was a new file:  unmap, hold in page cache, close
void BGFileCloser2(void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_WILLNEED);
#endif

    if (0 != file_ptr->unused_)
        ftruncate(file_ptr->fd_, file_ptr->offset_ + file_ptr->length_ - file_ptr->unused_);

    close(file_ptr->fd_);
    delete file_ptr;

    return;

}   // BGFileCloser2


// this was a reference file:  (currently unused)
void
BGFileUnmapper(
    void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_DONTNEED);
#endif

    delete file_ptr;

    return;

}   // BGFileUnmapper


// this was a new file:  RiakMmapFile
void
BGFileUnmapper2(
    void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_WILLNEED);
#endif

    delete file_ptr;

    return;

}   // BGFileUnmapper2

}  // namespace


static RiakEnv * sEnvArray;
static unsigned sEnvCreated=0, sEnvRequested;

static void
InitRiakEnv()
{
    int loop;

    // really want an odd number of thread blocks
    sEnvCreated=sEnvRequested;
    if (0==(sEnvCreated % 2))
        sEnvCreated+=1;

    sEnvArray=new RiakEnv[sEnvCreated];

    pthread_rwlock_init(&gThreadLock0, NULL);
    pthread_rwlock_init(&gThreadLock1, NULL);

}


Env*
RiakDefaultEnv(
    unsigned RequestedBlocks)   //!< how many environments/thread blocks to create
{
    static unsigned count(0);
    static pthread_once_t sOnce(PTHREAD_ONCE_INIT);

    // only first caller's RequestedBlocks actually used
    sEnvRequested=RequestedBlocks;
    pthread_once(&sOnce, InitRiakEnv);
    ++count;

  return &sEnvArray[count % sEnvCreated];

}

}  // namespace leveldb
