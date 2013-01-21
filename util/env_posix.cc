// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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
#include <time.h>
#include <unistd.h>
#if defined(LEVELDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/logging.h"
#include "util/posix_logger.h"
#include "db/dbformat.h"
#include "leveldb/perf_count.h"


#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
#define HAVE_FADVISE
#endif

namespace leveldb {

pthread_rwlock_t gThreadLock0;
pthread_rwlock_t gThreadLock1;

pthread_mutex_t gThrottleMutex;

#define THROTTLE_INTERVALS 63
#define THROTTLE_SECONDS 60
#define THROTTLE_TIME THROTTLE_SECONDS*1000000
#define THROTTLE_SCALING 17

struct ThrottleData_t
{
    uint64_t m_Micros;
    uint64_t m_Keys;
    uint64_t m_Backlog;
    uint64_t m_Compactions;
} gThrottleData[THROTTLE_INTERVALS];
uint64_t gThrottleRate;

static void *
ThrottleThread(
void * arg)
{
    Env * env;
    uint64_t tot_micros, tot_keys, tot_backlog, tot_compact;
    int replace_idx, loop;
    uint64_t new_throttle;

    env=(Env *)arg;
    replace_idx=2;

    while(1)
    {
        // sleep 5 minutes
        env->SleepForMicroseconds(THROTTLE_TIME);

        pthread_mutex_lock(&gThrottleMutex);
        gThrottleData[replace_idx]=gThrottleData[1];
        memset(&gThrottleData[1], 0, sizeof(gThrottleData[1]));
        pthread_mutex_unlock(&gThrottleMutex);

        tot_micros=0;
        tot_keys=0;
        tot_backlog=0;
        tot_compact=0;

        // this could be faster by keeping running totals and
        //  subtracting [replace_idx] before copying [0] into it,
        //  then adding new [replace_idx].  But that needs more
        //  time for testing.
        for (loop=2; loop<THROTTLE_INTERVALS; ++loop)
        {
            tot_micros+=gThrottleData[loop].m_Micros;
            tot_keys+=gThrottleData[loop].m_Keys;
            tot_backlog+=gThrottleData[loop].m_Backlog;
            tot_compact+=gThrottleData[loop].m_Compactions;
        }   // for

	// non-level0 data available?
        if (0!=tot_keys)
        {
            if (0==tot_compact)
                tot_compact=1;

            // average write time for level 1+ compactions per key
            //   times the average number of tasks waiting
            new_throttle=(tot_micros / tot_keys)
                * (tot_backlog / tot_compact);

        }   // if

	// attempt to most recent level0
	//  (only use most recent level0 until level1+ data becomes available,
	//   useful on restart of heavily loaded server)
	else if (0!=gThrottleData[0].m_Keys && 0!=gThrottleData[0].m_Compactions)
	{
            pthread_mutex_lock(&gThrottleMutex);
            new_throttle=(gThrottleData[0].m_Micros / gThrottleData[0].m_Keys)
	      * (gThrottleData[0].m_Backlog / gThrottleData[0].m_Compactions);
            pthread_mutex_unlock(&gThrottleMutex);
	}   // else if
        else
        {
            new_throttle=0;
        }   // else

        // change the throttle slowly
        if (gThrottleRate < new_throttle)
            gThrottleRate+=(new_throttle - gThrottleRate)/THROTTLE_SCALING;
        else
            gThrottleRate-=(gThrottleRate - new_throttle)/THROTTLE_SCALING;

        gPerfCounters->Set(ePerfThrottleGauge, gThrottleRate);
        gPerfCounters->Add(ePerfThrottleCounter, gThrottleRate*THROTTLE_SECONDS);

        // prepare for next interval
        pthread_mutex_lock(&gThrottleMutex);
        memset(&gThrottleData[0], 0, sizeof(gThrottleData[0]));
        pthread_mutex_unlock(&gThrottleMutex);

        ++replace_idx;
        if (THROTTLE_INTERVALS==replace_idx)
            replace_idx=2;

    }   // while

    return(NULL);

}   // ThrottleThread



namespace {

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));
}

// background routines to close and/or unmap files
static void BGFileCloser(void* file_info);
static void BGFileCloser2(void* file_info);
// currently unused static void BGFileUnmapper(void* file_info);
static void BGFileUnmapper2(void* file_info);

// data needed by background routines for close/unmap
struct BGCloseInfo
{
    int fd_;
    void * base_;
    size_t offset_;
    size_t length_;
    size_t unused_;

    BGCloseInfo(int fd, void * base, size_t offset, size_t length, size_t unused)
        : fd_(fd), base_(base), offset_(offset), length_(length), unused_(unused) {};
};

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

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
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

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
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  int fd_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length, int fd)
      : filename_(fname), mmapped_region_(base), length_(length), fd_(fd)
  {
      gPerfCounters->Inc(ePerfROFileOpen);

#if defined(HAVE_FADVISE)
    posix_fadvise(fd_, 0, length_, POSIX_FADV_RANDOM);
#endif
  }
  virtual ~PosixMmapReadableFile()
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
class PosixMmapFile : public WritableFile {
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
          Env::Default()->Schedule(&BGFileUnmapper2, ptr, 4);
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
  PosixMmapFile(const std::string& fname, int fd,
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

    gPerfCounters->Inc(ePerfRWFileOpen);
  }


  ~PosixMmapFile() {
    if (fd_ >= 0) {
      PosixMmapFile::Close();
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

class PosixFileLock : public FileLock {
 public:
  int fd_;
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
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
      *result = new PosixSequentialFile(fname, f);
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
            *result = new PosixMmapReadableFile(fname, base, size, fd);
        } else {
          s = IOError(fname, errno);
          close(fd);
        }
      }
#endif
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
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
      *result = new PosixMmapFile(fname, fd, page_size_);
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
          *result = new PosixMmapFile(fname, fd, page_size_, size);
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
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual void Schedule(void (*function)(void*), void* arg, int state=0, bool imm_flag=false, int priority=0);

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
      *result = new PosixLogger(f, &PosixEnv::gettid);
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
        ts.tv_nsec=(micros - ts.tv_sec*1000000) *1000;

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

    virtual void SetWriteRate(uint64_t Micros, uint64_t Keys, bool IsLevel0)
  {
      PthreadCall("lock", pthread_mutex_lock(&mu_));

      if (IsLevel0)
      {
          gThrottleData[0].m_Micros+=Micros;
          gThrottleData[0].m_Keys+=Keys;
          gThrottleData[0].m_Backlog+=GetBackgroundBacklog();
          gThrottleData[0].m_Compactions+=1;

          gPerfCounters->Add(ePerfThrottleMicros0, Micros);
          gPerfCounters->Add(ePerfThrottleKeys0, Keys);
          gPerfCounters->Add(ePerfThrottleBacklog0, GetBackgroundBacklog());
          gPerfCounters->Inc(ePerfThrottleCompacts0);
      }   // if

      else
      {
          gThrottleData[1].m_Micros+=Micros;
          gThrottleData[1].m_Keys+=Keys;
          gThrottleData[1].m_Backlog+=GetBackgroundBacklog();
          gThrottleData[1].m_Compactions+=1;

          gPerfCounters->Add(ePerfThrottleMicros1, Micros);
          gPerfCounters->Add(ePerfThrottleKeys1, Keys);
          gPerfCounters->Add(ePerfThrottleBacklog1, GetBackgroundBacklog());
          gPerfCounters->Inc(ePerfThrottleCompacts1);
      }   // else

      PthreadCall("unlock", pthread_mutex_unlock(&mu_));

      return;
  };

  virtual uint64_t GetWriteRate() const {return(gThrottleRate);};
#if 0
  virtual uint64_t SmoothWriteRate(uint64_t Rate)
  {
      uint64_t ret_val;

      PthreadCall("lock", pthread_mutex_lock(&gThrottleMutex));

      // precaution against bad timers, failing drives, and floppy disks
      if (Rate < 0xFFFFF && 0 != Rate)
      {
#if 0
          // scale up slowly ... averaging entire system
          if (gThrottleSum < Rate)
              gThrottleSum+=(Rate - gThrottleSum)/101;

          // scale down to a lower rate slowly
          else
              gThrottleSum-=(gThrottleSum - Rate)/101;
#else
          gThrottleSum[0]+=Rate;
          ++gThrottleCount[0];
#endif
      }   // if

      ret_val=(0!=(gThrottleCountPre+gThrottleCount[0]
                  ? (gThrottleSumPre+gThrottleSum[0])/(gThrottleCountPre+gThrottleCount[0])
                  : 0));

      PthreadCall("unlock", pthread_mutex_unlock(&gThrottleMutex));

      return(ret_val);
  };
#endif

 private:
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      exit(1);
    }
  }

  // BGThread() is the body of the background thread
  void BGThread();
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  size_t page_size_;
  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;     // normal compactions
  pthread_t bgthread2_;    // level 0 to level 1 compactions
  pthread_t bgthread3_;    // background unmap / close
  pthread_t bgthread4_;    // imm_ to level 0 compactions
  bool started_bgthread_;
  volatile int bg_backlog_;// count of items on 3 compaction queues
  volatile int bg_active_; // count of threads actually working compaction
  int64_t clock_res_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); int priority;};
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;     //normal compactions
  BGQueue queue2_;    // level 0 to level 1 compactions
  BGQueue queue3_;    //background unmap / close
  BGQueue queue4_;    // imm_ to level 0 compactions

  void InsertQueue2(struct PosixEnv::BGItem & item);

  void SetBacklog()
  {
      uint32_t cur_backlog;

      // mutex mu_ is assumed locked.
      cur_backlog=queue4_.size()+queue2_.size()+queue_.size()+bg_active_;

#if 0    // leave all smoothing to exponential formula
      // scale up moderately
      if (bg_backlog_ < cur_backlog)
              bg_backlog_+=(cur_backlog - bg_backlog_)/3;

      // scale down slower than up
      else
          bg_backlog_-=(bg_backlog_ - cur_backlog)/5;
#else
      bg_backlog_=cur_backlog;
#endif

      return;
  };

  volatile uint64_t write_rate_usec_; // recently experienced average time to
                                      // write one key during background compaction

};

PosixEnv::PosixEnv() : page_size_(getpagesize()),
                       started_bgthread_(false),
                       bg_backlog_(0), bg_active_(0),
                       clock_res_(1), write_rate_usec_(0)
{
  struct timespec ts;

#if _POSIX_TIMERS >= 200801L
  clock_getres(CLOCK_MONOTONIC, &ts);
  clock_res_=ts.tv_sec*1000000+ts.tv_nsec/1000;
  if (0==clock_res_)
      ++clock_res_;
#endif

  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
}

// state: 0 - legacy/default, 4 - close/unmap, 2 - test for queue switch, 1 - imm/high priority
void
PosixEnv::Schedule(
    void (*function)(void*),
    void* arg,
    int state,
    bool imm_flag,
    int priority)
{
    PthreadCall("lock", pthread_mutex_lock(&mu_));

    // Start background thread if necessary
    if (!started_bgthread_) {
        started_bgthread_ = true;

        // future, set SCHED_FIFO ... assume application at priority 50
        /// legacy compaction thread priority 45, 5 points lower
        PthreadCall(
            "create thread",
            pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this));

        /// level0 compaction, speed thread priority 60
        PthreadCall(
            "create thread 2",
            pthread_create(&bgthread2_, NULL,  &PosixEnv::BGThreadWrapper, this));

        /// close / unmap thread priority 47, higher than compaction but lower than application
        PthreadCall(
            "create thread 3",
            pthread_create(&bgthread3_, NULL,  &PosixEnv::BGThreadWrapper, this));

        /// imm->level0 dump
        PthreadCall(
            "create thread 4",
            pthread_create(&bgthread4_, NULL,  &PosixEnv::BGThreadWrapper, this));
    }

    // If the queue is currently empty, the background thread may currently be
    // waiting.
// 11/20/12 - have seen background threads stuck with full queues and threads waiting
//    if (queue_.empty() || queue2_.empty() || queue3_.empty() || queue4_.empty())
    {
        PthreadCall("broadcast", pthread_cond_broadcast(&bgsignal_));
    }

    if (0 != state)
    {
        BGQueue::iterator it;
        bool found;

        // close / unmap memory mapped files
        if (4==state)
        {
            if (queue3_.empty())
                PthreadCall("broadcast", pthread_cond_broadcast(&bgsignal_));

            queue3_.push_back(BGItem());
            queue3_.back().function = function;
            queue3_.back().arg = arg;
        }   // if

        // only "switch" lists if found waiting on a lower priority compaction list
        else if (2==state)
        {
            for (found=false, it=queue_.begin(); queue_.end()!=it && !found; ++it)
            {
                found= (it->arg == arg);
                if (found)
                {
                    BGQueue::iterator del_it;

                    del_it=it;
                    ++it;

                    queue_.erase(del_it);
                    if (imm_flag)
                    {
                        queue4_.push_back(BGItem());
                        queue4_.back().function = function;
                        queue4_.back().arg = arg;
                    }   // if
                    else
                    {
                        BGItem item={arg, function, priority};

                        InsertQueue2(item);
                    }   // else
                }   // if
            }   // for

            if (!found && imm_flag)
            {
                for (found=false, it=queue2_.begin(); queue2_.end()!=it && !found; ++it)
                {
                    found= (it->arg == arg);
                    if (found)
                    {
                        BGQueue::iterator del_it;

                        del_it=it;
                        ++it;

                        queue2_.erase(del_it);

                        queue4_.push_back(BGItem());
                        queue4_.back().function = function;
                        queue4_.back().arg = arg;
                    }   // if
                }   // for
            }   // if
        }   // if

        // state 1, adding imm_ or level 0 (nothing already scheduled)
        else
        {
            if (imm_flag)
            {
                queue4_.push_back(BGItem());
                queue4_.back().function = function;
                queue4_.back().arg = arg;
            }   // if
            else
            {
                BGItem item={arg, function, priority};

                InsertQueue2(item);
            }   // else
        }   // else
    }   // if
    else
    {
        // low priority compaction (not imm, not level0)
        queue_.push_back(BGItem());
        queue_.back().function = function;
        queue_.back().arg = arg;
    }   // else

    SetBacklog();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

/**
 * Poor man's std::priority_queue.  Said container appeared to not
 * support direct access to underlying type ... which is needed.
 */
void
PosixEnv::InsertQueue2(
    PosixEnv::BGItem & item)
{
    BGQueue::iterator it;
    bool looking;

    // this is slow and painful with deque as underlying container
    for (it=queue2_.begin(), looking=true; queue2_.end()!=it && looking; ++it)
    {
        if (it->priority<item.priority)
        {
            looking=false;
            queue2_.insert(it, item);
        }   // if
    }   // for

    if (looking)
        queue2_.push_back(item);

    return;

}   // Posix::InsertQueue2


void PosixEnv::BGThread()
{
    BGQueue * queue_ptr;

    // avoid race condition of whether or not all 4 thread creations
    //  have completed AND set bgthreadX_ values
    PthreadCall("lock", pthread_mutex_lock(&mu_));

    // pick source of thread's work
    if (bgthread4_==pthread_self())
        queue_ptr=&queue4_;
    else if (bgthread3_==pthread_self())
        queue_ptr=&queue3_;
    else if (bgthread2_==pthread_self())
        queue_ptr=&queue2_;
    else
        queue_ptr=&queue_;
    PthreadCall("unlock", pthread_mutex_unlock(&mu_));

    while (true)
    {
        // Wait until there is an item that is ready to run
        PthreadCall("lock", pthread_mutex_lock(&mu_));

        // ignore bg_active_ first time through loop (and if somehow corrupted)
        if (0<bg_active_)
        {
            --bg_active_;
            SetBacklog();
        }   // if
        else
            bg_active_=0;

        while (queue_ptr->empty()) {
            PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
        }

        void (*function)(void*) = queue_ptr->front().function;
        void* arg = queue_ptr->front().arg;
        queue_ptr->pop_front();

        ++bg_active_;
        SetBacklog();

        PthreadCall("unlock", pthread_mutex_unlock(&mu_));

        if (bgthread4_==pthread_self())
            gPerfCounters->Inc(ePerfBGCompactImm);
        else if (bgthread3_==pthread_self())
            gPerfCounters->Inc(ePerfBGCloseUnmap);
        else if (bgthread2_==pthread_self())
            gPerfCounters->Inc(ePerfBGCompactLevel0);
        else
            gPerfCounters->Inc(ePerfBGNormal);

        (*function)(arg);
    }   // while
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

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
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
    gPerfCounters->Inc(ePerfROFileClose);

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

    gPerfCounters->Inc(ePerfRWFileClose);

    return;

}   // BGFileCloser2

#if 0  // currently unused, remove due to compiler warning
// this was a reference file:  unmap, purge page cache
void BGFileUnmapper(void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_DONTNEED);
#endif

    delete file_ptr;
    gPerfCounters->Inc(ePerfROFileUnmap);

    return;

}   // BGFileUnmapper
#endif

// this was a new file:  unmap, hold in page cache
void BGFileUnmapper2(void * arg)
{
    BGCloseInfo * file_ptr;

    file_ptr=(BGCloseInfo *)arg;

    munmap(file_ptr->base_, file_ptr->length_);

#if defined(HAVE_FADVISE)
    posix_fadvise(file_ptr->fd_, file_ptr->offset_, file_ptr->length_, POSIX_FADV_WILLNEED);
#endif

    delete file_ptr;
    gPerfCounters->Inc(ePerfRWFileUnmap);

    return;

}   // BGFileUnmapper2

}  // namespace

// how many blocks of 4 priority background threads/queues
/// for riak, make sure this is an odd number (and especially not 4)
#define THREAD_BLOCKS 5

static bool HasSSE4_2();

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env[THREAD_BLOCKS];
static unsigned count=0;
static void InitDefaultEnv()
{
    int loop;
    pthread_t tid;

    for (loop=0; loop<THREAD_BLOCKS; ++loop)
    {
        default_env[loop]=new PosixEnv;
    }   // for

    pthread_rwlock_init(&gThreadLock0, NULL);
    pthread_rwlock_init(&gThreadLock1, NULL);
    pthread_mutex_init(&gThrottleMutex, NULL);

    memset(&gThrottleData, 0, sizeof(gThrottleData));
    gThrottleRate=0;

    pthread_create(&tid, NULL,  &ThrottleThread, default_env[0]);

    // force the loading of code for both filters in case they
    //  are hidden in a shared library
    const FilterPolicy * ptr;
    ptr=NewBloomFilterPolicy(16);
    delete ptr;
    ptr=NewBloomFilterPolicy2(16);
    delete ptr;

    if (HasSSE4_2())
        crc32c::SwitchToHardwareCRC();

    PerformanceCounters::Init(false);

}

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  ++count;
  return default_env[count % THREAD_BLOCKS];
}


static bool
HasSSE4_2()
{
#if defined(__x86_64__)
    uint64_t ecx;
    ecx=0;

    __asm__ __volatile__
        ("mov %%rbx, %%rdi\n\t" /* 32bit PIC: don't clobber ebx */
         "mov $1,%%rax\n\t"
         "cpuid\n\t"
         "mov %%rdi, %%rbx\n\t"
         : "=c" (ecx)
         :
         : "%rax", "%rbx", "%rdx", "%rdi" );

    return( 0 != (ecx & 1<<20));
#else
    return(false);
#endif

}   // HasSSE4_2



}  // namespace leveldb
