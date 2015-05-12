// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <time.h>
#include <algorithm>
#include <errno.h>
#include <math.h>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/level_size_cs.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/db_list.h"
#include "util/coding.h"
#include "util/flexcache.h"
#include "util/hot_threads.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/thread_tasks.h"
#include "util/throttle.h"
#include "util/at_exit.h"
#include "leveldb/perf_count.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

using namespace std;

namespace leveldb {

static
Status GetStatus(std::exception &e){
  if ( Status *s = dynamic_cast<Status*>(&e) ){
    return *s;
  }
  return Status::IOError(e.what());
}

// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

Value::~Value() {}

class StringValue : public Value {
 public:
  explicit StringValue(std::string& val) : value_(val) {}
  ~StringValue() {}

  StringValue& assign(const char* data, size_t size) {
    value_.assign(data, size);
    return *this;
  }

  StringValue& append(const char* data, size_t size) {
    value_.append(data, size);
    return *this;
  }

  StringValue & reserve(size_t size) {
    value_.reserve(size);
    return *this;
  }

 private:
  std::string& value_;
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src,
                        Cache * block_cache) {
  std::string tiered_dbname;
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,            20,     50000);
  ClipToRange(&result.write_buffer_size,         64<<10, 1<<30);
  ClipToRange(&result.block_size,                1<<10,  4<<20);

  if (src.limited_developer_mem)
      gMapSize=2*1024*1024L;

  // alternate means to change gMapSize ... more generic
  if (0!=src.mmap_size)
      gMapSize=src.mmap_size;

  if (gMapSize < result.write_buffer_size) // let unit tests be smaller
      result.write_buffer_size=gMapSize;

  // Validate tiered storage options
  tiered_dbname=MakeTieredDbname(dbname, result);

  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(tiered_dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(tiered_dbname), OldInfoLogFileName(tiered_dbname));
    Status s = src.env->NewLogger(InfoLogFileName(tiered_dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }

  if (result.block_cache == NULL) {
      result.block_cache = block_cache;
  }

  return result;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
  :DBImpl( options, dbname, std::make_shared<DoubleCache>(options) )
{

}

static
void EnsureOk(Status s){
  if (!s.ok())
    throw s;
}

DBImpl::DBImpl(
    const Options& options,
    const std::string& dbname,
    std::shared_ptr<DoubleCache> cache)

    : double_cache(cache),
      env_(options.env),
      internal_comparator_(options.comparator),
      internal_filter_policy_(options.filter_policy),
      options_(SanitizeOptions(
          dbname, &internal_comparator_, &internal_filter_policy_,
          options, block_cache())),
      owns_info_log_(options_.info_log != options.info_log),
      owns_cache_(options_.block_cache != options.block_cache),
      // TODO: shouldn't be tiered_dbname from SanitizeOptions?
      dbname_(options_.tiered_fast_prefix),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(make_shared<MemTable>(internal_comparator_)),
      logfile_number_(0),
      tmp_batch_(new WriteBatch),
      throttle_end(0),
      running_compactions_(0),
      increaseBlockSizeInterval_(std::chrono::minutes(5)),
      current_block_size_step_(0),
      table_cache_( new TableCache(dbname_, &options_, file_cache(), *double_cache.get()) ),
      versions_( new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_) ),
      rangeDeletes_(options.enableRangeDeletes? new RangeDeletes(dbname, options.env, options.comparator) : nullptr)
{
  DBList()->AddDB(this, options_.is_internal_db);
  gFlexCache.SetTotalMemory(options_.total_leveldb_mem);
  // switch global for everyone ... tacky implementation for now
  gFadviseWillNeed=options_.fadvise_willneed;

  options_.Dump(options_.info_log);
  Log(options_.info_log,"               File cache size: %zd", double_cache->GetCapacity(true));
  Log(options_.info_log,"              Block cache size: %zd", double_cache->GetCapacity(false));

  try{
    MutexLock dbl(&mutex_);
    VersionEdit edit;
    Status s;
    // 4 level0 files at 2Mbytes and 2Mbytes of block cache
    //  (but first level1 file is likely to thrash)
    //  ... this value is AFTER write_buffer and 40M for recovery log and LOG
    //if (!options.limited_developer_mem && impl->GetCacheCapacity() < flex::kMinimumDBMemory)
    //    s=Status::InvalidArgument("Less than 10Mbytes per database/vnode");
    s = Recover(&edit); // Handles create_if_missing, error_if_exists
    EnsureOk(s);
    uint64_t new_log_number = versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWriteOnlyFile(LogFileName(dbname, new_log_number),
                                      &lfile, options.env->RecoveryMmapSize(&options));
    EnsureOk(s);
    log_.reset(new log::Writer(lfile));
    edit.SetLogNumber(new_log_number);
    logfile_number_ = new_log_number;
    s = versions_->LogAndApply(&edit, &mutex_);
    EnsureOk(s);
    DeleteObsoleteFiles();
    //CheckCompactionState();
    unique_ptr<LevelSizeCS> lvlSizeCS(new LevelSizeCS());
    lvlSizeCS->attachTo(this);
    compactionStrategies_.push_back(move(lvlSizeCS));
  }
  catch(...){
    Destruct();
    throw;
  }
}

void DBImpl::Destruct(){
  DBList()->ReleaseDB(this, options_.is_internal_db);

  // Wait for background work to finish
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (true){
    MutexLock l(&mutex_);
    if (!IsBackroundJobs())
      break;
    bg_cv_.Wait();
  }
  // make sure flex cache knows this db is gone
  //  (must follow ReleaseDB() call ... see above)
  gFlexCache.RecalculateAllocations();

  delete versions_;
  delete tmp_batch_;
  log_.reset();
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }
}

DBImpl::~DBImpl() {
  Destruct();
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file, 4*1024L);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok())
      s = file->Close();
  }
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }

  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles()
{
  // Only run this routine when down to one
  //  simultaneous compaction
  if (RunningCompactionCount()<2)
  {
      // each caller has mutex, we need to release it
      //  since this disk activity can take a while
      mutex_.AssertHeld();

      // Make a set of all of the live files
      std::set<uint64_t> live = pending_outputs_;
      versions_->AddLiveFiles(&live);

      // prune the database root directory
      std::vector<std::string> filenames;
      env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
      for (size_t i = 0; i < filenames.size(); i++) {
          KeepOrDelete(filenames[i], -1, live);
      }   // for

      // prune the table file directories
      for (int level=0; level<config::kNumLevels; ++level)
      {
          std::string dirname;

          filenames.clear();
          dirname=MakeDirName2(options_, level, "sst");
          env_->GetChildren(dirname, &filenames); // Ignoring errors on purpose
          for (size_t i = 0; i < filenames.size(); i++) {
              KeepOrDelete(filenames[i], level, live);
          }   // for
      }   // for
  }   // if
}

void
DBImpl::KeepOrDelete(
    const std::string & Filename,
    int Level,
    const std::set<uint64_t> & Live)
{
  uint64_t number;
  FileType type;
  bool keep = true;

  if (ParseFileName(Filename, &number, &type))
  {
      switch (type)
      {
          case kLogFile:
              keep = ((number >= versions_->LogNumber()) ||
                      (number == versions_->PrevLogNumber()));
              break;

          case kDescriptorFile:
              // Keep my manifest file, and any newer incarnations'
              // (in case there is a race that allows other incarnations)
              keep = (number >= versions_->ManifestFileNumber());
              break;

          case kTableFile:
              keep = (Live.find(number) != Live.end());
              break;

          case kTempFile:
              // Any temp files that are currently being written to must
              // be recorded in pending_outputs_, which is inserted into "Live"
              keep = (Live.find(number) != Live.end());
          break;

          case kCurrentFile:
          case kDBLockFile:
          case kInfoLogFile:
              keep = true;
              break;
      }   // switch

      if (!keep)
      {
          if (type == kTableFile) {
              // temporary hard coding of extra overlapped
              //  levels
              table_cache_->Evict(number, (Level<config::kNumOverlapLevels));
          }
          Log(options_.info_log, "Delete type=%d #%lld\n",
              int(type),
              static_cast<unsigned long long>(number));

          if (-1!=Level)
          {
              std::string file;

              file=TableFileName(options_, number, Level);
              env_->DeleteFile(file);
          }   // if
          else
          {
              env_->DeleteFile(dbname_ + "/" + Filename);
          }   // else
      }   // if
  }   // if
} // DBImpl::KeepOrDelete


Status DBImpl::Recover(VersionEdit* edit) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(options_.tiered_fast_prefix);
  env_->CreateDir(options_.tiered_slow_prefix);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  // read manifest
  s = versions_->Recover();

  // Verify Riak 1.3 directory structure created and ready
  if (s.ok() && !TestForLevelDirectories(env_, options_, versions_->current()))
  {
      int level;
      std::string old_name, new_name;

      if (options_.create_if_missing)
      {
          // move files from old heirarchy to new
          s=MakeLevelDirectories(env_, options_);
          if (s.ok())
          {
              for (level=0; level<config::kNumLevels && s.ok(); ++level)
              {
                  const std::vector<FileMetaData*> & level_files(versions_->current()->GetFileList(level));
                  std::vector<FileMetaData*>::const_iterator it;

                  for (it=level_files.begin(); level_files.end()!=it && s.ok(); ++it)
                  {
                      new_name=TableFileName(options_, (*it)->number, level);

                      // test for partial completion
                      if (!env_->FileExists(new_name.c_str()))
                      {
                          old_name=TableFileName(options_, (*it)->number, -2);
                          s=env_->RenameFile(old_name, new_name);
                      }   // if
                  }   // for
              }   // for
          }   // if
          else
              return s;
      }   // if
      else
      {
          return Status::InvalidArgument(
              dbname_, "level directories do not exist (create_if_missing is false)");
      }   // else
  }   // if


  if (s.ok()) {
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
      return s;
    }
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)
          && type == kLogFile
          && ((number >= min_log) || (number == prev_log))) {
        logs.push_back(number);
      }
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size() && s.ok(); i++) {
      s = RecoverLogFile(logs[i], edit, &max_sequence);

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]);
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
  }

  return s;
}


void DBImpl::CheckCompactionState()
{
//    mutex_.AssertHeld();
//    bool log_flag, need_compaction;

//    // Verify Riak 1.4 level sizing, run compactions to fix as necessary
//    //  (also recompacts hard repair of all files to level 0)

//    log_flag=false;
//    need_compaction=false;

//    // loop on pending background compactions
//    //  reminder: mutex_ is held
//    do
//    {
//        int level;

//        // wait out executing compaction (Wait gives mutex to compactions)
//        if (IsCompactionScheduled())
//            bg_cv_.Wait();

//        for (level=0, need_compaction=false;
//             level<config::kNumLevels && !need_compaction;
//             ++level)
//        {
//            if (versions_->IsLevelOverlapped(level)
//                && config::kL0_SlowdownWritesTrigger<=versions_->NumLevelFiles(level))
//            {
//                need_compaction=true;
//                MaybeScheduleCompaction();
//                if (!log_flag)
//                {
//                    log_flag=true;
//                    Log(options_.info_log, "Cleanup compactions started ... DB::Open paused");
//                }   // if
//            }   //if
//        }   // for

//    } while(IsCompactionScheduled() && need_compaction);

//    if (log_flag)
//        Log(options_.info_log, "Cleanup compactions completed ... DB::Open continuing");

//    // prior code only called this function instead of CheckCompactionState
//    //  (duplicates original Google functionality)
//    else
//        MaybeScheduleCompaction();

//    return;

}  // DBImpl::CheckCompactionState()


Status DBImpl::RecoverLogFile(uint64_t log_number,
                              VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  unique_ptr<MemTable> mem;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (!mem) {
      mem = make_unique<MemTable>(internal_comparator_);
    }
    status = WriteBatchInternal::InsertInto(&batch, mem.get());
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      status = WriteLevel0Table(mem.get(), edit, NULL);
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
      mem.reset();
    }
  }

  if (status.ok() && mem) {
    status = WriteLevel0Table(mem.get(), edit, NULL);
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  delete file;
  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  meta.level = 0;
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  SequenceNumber smallest_snapshot;

  if (snapshots_.empty()) {
    smallest_snapshot = versions_->LastSequence();
  } else {
    smallest_snapshot = snapshots_.oldest()->number_;
  }

  Status s;
  {
    Options local_options;
    local_options=options_;
    local_options.compression=kNoCompression;
    local_options.block_size=blockSize(0);

    mutex_.Unlock();
    Log(options_.info_log, "Level-0 table #%llu: started",
        (unsigned long long) meta.number);

    // want the data slammed to disk as fast as possible,
    //  no compression for level 0.
    s = BuildTable(dbname_, env_, local_options, user_comparator(), table_cache_, iter, &meta, smallest_snapshot);

    Log(options_.info_log, "Level-0 table #%llu: %llu bytes, %llu keys %s",
        (unsigned long long) meta.number,
        (unsigned long long) meta.file_size,
        (unsigned long long) meta.num_entries,
      s.ToString().c_str());
    mutex_.Lock();
  }

  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();

    edit->AddFile(level, meta.number, meta.file_size,
                      meta.smallest, meta.largest);
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);

  if (0!=meta.num_entries && s.ok())
  {
      // This SetWriteRate() call removed because this thread
      //  has priority (others blocked on mutex) and thus created
      //  misleading estimates of disk write speed
      // 2x since mem to disk, not disk to disk
      //      env_->SetWriteRate(2*stats.micros/meta.num_entries);
      // 2x since mem to disk, not disk to disk
      // env_->SetWriteRate(2*stats.micros/meta.num_entries);
  }   // if

  // Riak adds extra reference to file, must remove it
  //  in this race condition upon close
  if (s.ok() && shutting_down_.Acquire_Load()) {
      versions_->GetTableCache()->Evict(meta.number, true);
  }

  return s;
}

void DBImpl::enqueueMemWrite()
{
  mutex_.AssertHeld();
  if (imm_){
    gPerfCounters->Inc(ePerfWriteWaitImm);
    waitImmWriteFinish();
    if (imm_) // some other thread already enqueued the memwrite, while lock was released
      return;
  }
  // Attempt to switch to a new memtable and trigger compaction of old
  assert(versions_->PrevLogNumber() == 0);
  uint64_t new_log_number = versions_->NewFileNumber();

  WritableFile* lfile = NULL;
  gPerfCounters->Inc(ePerfWriteNewMem);
  Status s = env_->NewWriteOnlyFile(LogFileName(dbname_, new_log_number), &lfile,
                             options_.env->RecoveryMmapSize(&options_));
  EnsureOk(s);
  logfile_number_ = new_log_number;
  log_.reset(new log::Writer(lfile));
  imm_ = mem_;
  mem_ = make_shared<MemTable>(internal_comparator_);
  immWrite_.add(bind(&DBImpl::BackgroundImmCompactCall, this));
}

void DBImpl::waitImmWriteFinish()
{
  mutex_.AssertHeld();
  if (!imm_)
    return;
  mutex_.Unlock();
  auto whenImmDone = immWrite_.addF([]{});
  whenImmDone.wait();
  mutex_.Lock();
}

Status DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_.get(), &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_.reset();
    DeleteObsoleteFiles();
  }

  return s;
}


Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_  && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_) {
      s = bg_error_;
    }
  }
  return s;
}

void
DBImpl::BackgroundImmCompactCall() {
  MutexLock l(&mutex_);
  assert(imm_);
  Status s;

  ++running_compactions_;
  gPerfCounters->Inc(ePerfBGCompactImm);

  if (!shutting_down_.Acquire_Load()) {
    s = CompactMemTable();
    if (!s.ok()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      Log(options_.info_log, "Waiting after background imm compaction error: %s",
          s.ToString().c_str());
      env_->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }
  }

  --running_compactions_;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.

  // retry imm compaction if failed and not shutting down
  if (!s.ok() && !shutting_down_.Acquire_Load())
  {
      immWrite_.add(bind(&DBImpl::BackgroundImmCompactCall, this));
  }

  bg_cv_.SignalAll();
}


namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  shared_ptr<MemTable> mem;
  shared_ptr<MemTable> imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  MutexLock l(state->mu);
  state->mem.reset();
  state->imm.reset();
  state->version->Unref();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot) {
  IterState* cleanup = new IterState;
  MutexLock l(&mutex_);
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  if (imm_)
     list.push_back(imm_->NewIterator());
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  return NewInternalIterator(ReadOptions(), &ignored);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  StringValue stringvalue(*value);
  return DBImpl::Get(options, key, &stringvalue);
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   Value* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  shared_ptr<MemTable> mem = mem_;
  shared_ptr<MemTable> imm = imm_;
  Version* current = versions_->current();
  current->Ref();

  RangeDeletes::DBSnapshot rds;
  if (rangeDeletes_)
    rds = rangeDeletes_->dbSnapshot();

  bool have_stat_update = false;
  Version::GetStats stats;

  SequenceNumber seq;
  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot, options_.translator);
    if (mem->Get(lkey, value, &seq, &s)) {
        gPerfCounters->Inc(ePerfGetMem);
    } else if (imm && imm->Get(lkey, value, &seq, &s)) {
        gPerfCounters->Inc(ePerfGetImm);
    } else {
      s = current->Get(options, lkey, value, &seq, &stats);
      have_stat_update = true;
      gPerfCounters->Inc(ePerfGetVersion);
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
      // no compactions initiated by reads, takes too long
      // MaybeScheduleCompaction();
  }
  current->Unref();

  if (rangeDeletes_ && rds.isDeleted(key, seq))
    s = Status::NotFound("deleted by a range delete");

  gPerfCounters->Inc(ePerfApiGet);

  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  Iterator* internal_iter = NewInternalIterator(options, &latest_snapshot);
  gPerfCounters->Inc(ePerfIterNew);
  return NewDBIterator(
      &dbname_, env_, user_comparator(), internal_iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot));
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  WriteBatch batch;
  batch.Put(key, val);
  return Write(o, &batch);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);

  // Negate the count to "ApiWrite"
  gPerfCounters->Dec(ePerfApiWrite);
  gPerfCounters->Inc(ePerfApiDelete);

  return Write(options, &batch);
}

Status DBImpl::Delete(const WriteOptions &, const Slice &fromKey, const Slice &toKey)
{
  if (!rangeDeletes_)
    return Status::NotSupported("Enable range deletes in options before using them");
  try{
    SequenceNumber seq;
    {
      MutexLock l(&mutex_);
      seq = versions_->LastSequence();
    }
    rangeDeletes_->addInterval(fromKey, toKey, seq);
    if ( numRangeDeleteIntervals(0) > compactionTarget_.numRangeDeletes() ){
      MutexLock l(&mutex_);
      enqueueCompaction(0);
    }
  } catch(exception &e){
    Log(options_.info_log, "range delete failed. %s", e.what() );
    return GetStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Status status;
  int throttle(0);

  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

  {  // place mutex_ within a block
     //  not changing tabs to ease compare to Google sources
  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;  // skips throttle ... maintenance unfriendly coding, bastards
  }

  // May temporarily unlock and wait.
  status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    {
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      if (status.ok() && options.sync) {
        status = log_->Sync();
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_.get());
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  gPerfCounters->Inc(ePerfApiWrite);

  // protect use of versions_ ... still within scope of mutex_ lock
  throttle=versions_->WriteThrottleUsec(IsBackroundJobs());
  }  // release  MutexLock l(&mutex_)


  // throttle on exit to reduce possible reordering
  if (0!=throttle)
  {
      uint64_t now, remaining_wait, new_end, batch_wait;
      int batch_count;

      /// slowing each call down sequentially
      MutexLock l(&throttle_mutex_);

      // server may have been busy since previous write,
      //  use only the remaining time as throttle
      now=env_->NowMicros();

      if (now < throttle_end)
      {

          remaining_wait=throttle_end - now;
          env_->SleepForMicroseconds(remaining_wait);
          new_end=now+remaining_wait+throttle;

          gPerfCounters->Add(ePerfThrottleWait, remaining_wait);
      }   // if
      else
      {
          remaining_wait=0;
          new_end=now + throttle;
      }   // else

      // throttle is per key write, how many in batch?
      //  (do not use batch count on internal db because of impact to AAE)
      batch_count=(!options_.is_internal_db && NULL!=my_batch ? WriteBatchInternal::Count(my_batch) : 1);
      if (0 < batch_count)  // unclear if Count() could return zero
          --batch_count;
      batch_wait=throttle * batch_count;

      // only wait on batch if extends beyond potential wait period
      if (now + remaining_wait < throttle_end + batch_wait)
      {
          remaining_wait=throttle_end + batch_wait - (now + remaining_wait);
          env_->SleepForMicroseconds(remaining_wait);
          new_end +=remaining_wait;

          gPerfCounters->Add(ePerfThrottleWait, remaining_wait);
      }   // if

      throttle_end=new_end;
  }   // if

  // throttle not needed, kill off old wait time
  else if (0!=throttle_end)
  {
      throttle_end=0;
  }   // else if

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *reuslt
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;

  fireOnWrite();

  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
        gPerfCounters->Inc(ePerfWriteError);
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= (int)config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
#if 0   // see if this impacts smoothing or helps (but keep the counts)
      env_->SleepForMicroseconds(1000);
#endif
      allow_delay = false;  // Do not delay a single write more than once
      gPerfCounters->Inc(ePerfWriteSleep);
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
        gPerfCounters->Inc(ePerfWriteNoWait);
      break;
    } else {
      enqueueMemWrite();
      force = false;   // Do not force another compaction if have room
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= (uint64_t)config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%zd",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "total-bytes") {
    char buf[50];
    uint64_t total = 0;
    for (int level = 0; level < config::kNumLevels; level++) {
      total += versions_->NumLevelBytes(level);
    }
    snprintf(buf, sizeof(buf), "%" PRIu64, total);
    value->append(buf);
    return true;
  } else if (in == "file-cache") {
    char buf[50];
    snprintf(buf, sizeof(buf), "%zd", double_cache->GetCapacity(true));
    value->append(buf);
    return true;
  } else if (in == "block-cache") {
    char buf[50];
    snprintf(buf, sizeof(buf), "%zd", double_cache->GetCapacity(false));
    value->append(buf);
    return true;
  } else if (-1!=gPerfCounters->LookupCounter(in.ToString().c_str())) {

      char buf[66];
      int index;

      index=gPerfCounters->LookupCounter(in.ToString().c_str());
      snprintf(buf, sizeof(buf), "%" PRIu64 , gPerfCounters->Value(index));
      value->append(buf);
      return(true);
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

void DBImpl::CompactRange(const Slice *begin, const Slice *end)
{

}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);

  // Negate the count to "ApiWrite"
  gPerfCounters->Dec(ePerfApiWrite);
  gPerfCounters->Inc(ePerfApiDelete);

  return Write(opt, &batch);
}

DB::~DB() { }

std::string DBImpl::GetFamilyPath(const std::string &family_name){
  return dbname_ + "/fml_" + family_name;
}

static
Status GetFamilyErrorStatus(std::exception &e){
  if ( dynamic_cast<std::out_of_range*>(&e) ){
    return Status::InvalidArgument("no such family was opened. call OpenFamily() first");
  }
  if ( Status *s = dynamic_cast<Status*>(&e) ){
    return *s;
  }
  return Status::InvalidArgument(e.what());
}

Status DBImpl::OpenFamily(const Options &options, const std::string &name)
{
  try{
    WriteLock scopedMtx(&families_mtx_);
    std::string fml_path = GetFamilyPath(name);
    std::unique_ptr< DBImpl > db(new DBImpl(options, fml_path, double_cache));
    if ( ! families_.emplace( std::make_pair(name, std::move(db)) ).second ){
      return Status::InvalidArgument("already opened");
    }
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::CloseFamily(const std::string &name)
{
  WriteLock scopedMtx(&families_mtx_);
  if ( families_.erase(name) == 0 )
    return Status::InvalidArgument("can't close the family");
  return Status::OK();
}

Status DBImpl::Put(const std::string &family, const WriteOptions &opt, const Slice &key, const Slice &value)
{
  try{
    ReadLock scopedMtx(&families_mtx_);
    DBImpl *fdb = families_.at(family).get();
    return fdb->Put(opt,key,value);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Delete(const std::string &family, const WriteOptions &o, const Slice &key)
{
  try{
    ReadLock scopedMtx(&families_mtx_);
    DBImpl *fdb = families_.at(family).get();
    return fdb->Delete(o, key);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Delete(const string &family, const WriteOptions &o, const Slice &fromKey, const Slice &toKey)
{
  try{
    ReadLock scopedMtx(&families_mtx_);
    DBImpl *fdb = families_.at(family).get();
    return fdb->Delete(o, fromKey, toKey);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Write(const std::string &family, const WriteOptions &o, WriteBatch *updates)
{
  try{
    ReadLock scopedMtx(&families_mtx_);
    DBImpl *fdb = families_.at(family).get();
    return fdb->Write(o, updates);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Get(const std::string &family, const ReadOptions &options, const Slice &key, std::string *value)
{
  try{
    ReadLock scopedMtx(&families_mtx_);
    DBImpl *fdb = families_.at(family).get();
    return fdb->Get(options, key, value);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DBImpl::Get(const std::string &family, const ReadOptions &options, const Slice &key, Value *value)
{
  ReadLock scopedMtx(&families_mtx_);
  try{
    DBImpl *fdb = families_.at(family).get();
    return fdb->Get(options, key, value);
  }
  catch(std::exception &e){
    return GetFamilyErrorStatus(e);
  }
  return Status::OK();
}

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;
  try{
    *dbptr = new DBImpl(options, dbname);
  }
  catch(Status &s){
    return s;
  }
  gPerfCounters->Inc(ePerfApiOpen);
  return Status::OK();
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Options options_tiered;
  std::string dbname_tiered;

  options_tiered=options;
  dbname_tiered=MakeTieredDbname(dbname, options_tiered);

  // Ignore error in case directory does not exist
  env->GetChildren(dbname_tiered, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname_tiered);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;

    // prune the table file directories
    for (int level=0; level<config::kNumLevels; ++level)
    {
        std::string dirname;

        filenames.clear();
        dirname=MakeDirName2(options_tiered, level, "sst");
        env->GetChildren(dirname, &filenames); // Ignoring errors on purpose
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type)) {
                Status del = env->DeleteFile(dirname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }   // if
            }   // if
        }   // for
        env->DeleteDir(dirname);
    }   // for

    filenames.clear();
    env->GetChildren(dbname_tiered, &filenames);
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname_tiered + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(options.tiered_fast_prefix);  // Ignore error in case dir contains other files
    env->DeleteDir(options.tiered_slow_prefix);  // Ignore error in case dir contains other files
  }
  return result;
}


Status DB::VerifyLevels() {return(Status::InvalidArgument("is_repair not set in Options before database opened"));};

// Riak specific repair
Status
DBImpl::VerifyLevels()
{
    Status result;

    // did they remember to open the db with flag set in options
    if (options_.is_repair)
    {
        InternalKey begin, end;
        bool overlap_found;
        int level;
        Version * ver;

        overlap_found=false;
        level=0;

        do
        {
            // get a copy of current version
            {
                MutexLock l(&mutex_);
                ver = versions_->current();
                ver->Ref();
            }

            // level is input and output (acts as cursor to progress)
            //  begin and end are outputs of function
            overlap_found=ver->VerifyLevels(level, begin, end);
            ver->Unref();
        } while(overlap_found);

    }   // if
    else
    {
        result=Status::InvalidArgument("is_repair not set in Options before database opened");
    }   // else

    return(result);

}   // VerifyLevels

bool
DBImpl::IsBackroundJobs()
{
  mutex_.AssertHeld();
  bool ret = std::accumulate(isCompactionSubmitted_.begin(), isCompactionSubmitted_.end(), false);
  return ret;
}


DataDictionary * NewDataDictionary() {
  return new DataDictionary;
}

void DeleteDataDictionary(DataDictionary * dd) {
  delete dd;
}


void DBImpl::addWriteListener(DBImpl::OnWrite f)
{
  mutex_.AssertHeld();
  onWrite_.push_back( std::move(f) );
}

void DBImpl::fireOnWrite()
{
  for ( auto &e : onWrite_ )
    e();
}

void DBImpl::addCompactionFinishedListener(DBImpl::OnCompactionFinished f)
{
  mutex_.AssertHeld();
  onCompactionFinished_.push_back( std::move(f) );
}

void DBImpl::fireOnCompactionFinished(int level)
{
  mutex_.AssertHeld();
  for ( auto &e : onCompactionFinished_ )
    e(level);
}

bool DBImpl::isCompactionSubmitted(int level){
  mutex_.AssertHeld();
  bool submitted = false;
  if ( level < isCompactionSubmitted_.size() )
    submitted |= isCompactionSubmitted_[level];
  level++;
  if ( level < isCompactionSubmitted_.size() )
    submitted |= isCompactionSubmitted_[level];
  return submitted;
}

void DBImpl::compactionSubmitted(int level)
{
  mutex_.AssertHeld();
  int nextLvl = level+1;
  if ( isCompactionSubmitted_.size() <= nextLvl )
    isCompactionSubmitted_.resize(nextLvl +1, false);
  isCompactionSubmitted_[level] = true;
  isCompactionSubmitted_[nextLvl] = true;
}

void DBImpl::compactionFinished(int level)
{
  mutex_.AssertHeld();
  isCompactionSubmitted_[level+1] = false;
  isCompactionSubmitted_[level] = false;
  fireOnCompactionFinished(level);
}

void DBImpl::enqueueCompaction(int level)
{
  mutex_.AssertHeld();
  if ( level >= config::kNumLevels-1 )
    return;
  if ( isCompactionSubmitted(level) )
    return; // No!
  auto fileList = [=](const Version *v) -> std::vector<FileMetaData*> {
    return v->GetFileList(level);
  };
  DBImpl::DropTheKey neverDrop = [](Slice, SequenceNumber) -> bool {
    return false;
  };
  auto *task = new CompactionTask(this, level);
  gCompactionThreads->Submit(task, true);
  compactionSubmitted(level);
}


size_t DBImpl::blockSize(int level)
{
  mutex_.AssertHeld();
  if ( increaseBlockSizeInterval_.isTime() && !double_cache->IsPlentySpaceAvailable()){
    if ( current_block_size_step_ < options_.block_size_steps )
          current_block_size_step_++;
  }
  return options_.block_size << (current_block_size_step_ + level/3); //every 3rd level increases block size *2
}

void DBImpl::compact(int level)
{
  try{
    AtExit signal([&]{
      MutexLock l(&mutex_);
      compactionFinished(level);
      bg_cv_.SignalAll();
    });

    if ( shutting_down_.Acquire_Load() )
      return;

    if (0==level)
        gPerfCounters->Inc(ePerfBGCompactLevel0);
    else
        gPerfCounters->Inc(ePerfBGNormal);

    Log(options_.info_log,
      "Started compacting level %d to %d", level, level+1 );

    RangeDeletes::LevelSnapshot rds;
    if ( rangeDeletes_ ){
      rds = rangeDeletes_->createSnapshot(level);
      if ( level == 0 ){
        MutexLock l(&mutex_);
        enqueueMemWrite();
        waitImmWriteFinish();
      }
    }

    const Version *currentVersion;
    {
      MutexLock l(&mutex_);
      currentVersion = versions_->current();
      const_cast<Version*>(currentVersion)->Ref();
    }
    AtExit releaseVer([&]{
      MutexLock l(&mutex_);
      const_cast<Version*>(currentVersion)->Unref();
    });

    // level we are compacting to is the last
    bool dropRD = rangeDeletes_ && currentVersion->getLastFilledLevel() <= level + 1;
    Options outOpt;
    const Comparator *userCmp;
    vector<FileMetaData*> inputs;
    vector<FileMetaData> createdFiles;
    unique_ptr<Iterator> mergingIterator;
    {
      userCmp = user_comparator();
      ReadOptions options;
      options.verify_checksums = options_.verify_compactions;
      options.fill_cache = false;
      options.is_compaction = true;
      options.info_log = options_.info_log;
      options.dbname = dbname_;
      options.env = env_;

      // range deletes have to be applied to all the files in the level we are moving the RD from
      // so it's necessary to always get them all
      inputs = currentVersion->GetFileList(level);
      if ( dropRD ){  // we must make sure that we have applied range deletes to
                      // all the files, if we are going to drop the range deletes.
        vector<FileMetaData*> nextLvlInputs = currentVersion->GetFileList(level+1);
        inputs.insert(inputs.end(), nextLvlInputs.begin(), nextLvlInputs.end());
      }
      else{
        vector<FileMetaData*> overlappedInputs;
        set<FileMetaData*> levelNextInputs;
        for (FileMetaData* in : inputs) {
          overlappedInputs.clear();
          currentVersion->GetOverlappingInputs(level+1, &in->smallest, &in->largest, &overlappedInputs );
          levelNextInputs.insert(overlappedInputs.begin(), overlappedInputs.end());
        }
        inputs.insert(inputs.end(), levelNextInputs.begin(), levelNextInputs.end());
      }
      vector<unique_ptr<Iterator>> itList;
      for ( auto in : inputs)
        itList.emplace_back( table_cache_->NewIterator(options, in->number, in->file_size, in->level) );
      vector<Iterator*> it;
      for (auto &p : itList)
        it.push_back(p.get());
      mergingIterator.reset( NewMergingIterator(&internal_comparator_, &it.front(), it.size()) );
      for (auto &p : itList)
        p.release();
      outOpt = options_;
      MutexLock l(&mutex_);
      outOpt.block_size = blockSize(level+1);
    }
    if ( !inputs.empty() )
    {
      CompactionOutput out(&createdFiles, &pending_outputs_, &outOpt, &mutex_, versions_, env_, level + 1, &compactionStats_);
      out.reset();
      string outKey;
      DataBuffer outVal;
      int lastLvl = currentVersion->getLastFilledLevel();
      auto addKey = [&](){
        if ( outKey.empty() )
          return;
        ParsedInternalKey outIKey;
        ParseInternalKey(outKey, &outIKey);
        if ( outIKey.type == kTypeDeletion && level + 1 == lastLvl )
          return;
        if ( rangeDeletes_ && rds.isDeleted(outIKey.user_key, outIKey.sequence) )
          return;
        out.add(outKey, outVal);
      };
      for (mergingIterator->SeekToFirst(); mergingIterator->Valid(); mergingIterator->Next())
      {
        if ( shutting_down_.Acquire_Load() )
          return;
        EnsureOk(mergingIterator->status());
        Slice key = mergingIterator->key();

        ParsedInternalKey ikey;
        if (!ParseInternalKey(key, &ikey))
          throw Status::Corruption("Key decoding error happened in compaction process");
        ParsedInternalKey outIKey;
        ParseInternalKey(outKey, &outIKey);
        if ( userCmp->Compare(ikey.user_key, outIKey.user_key) == 0 ){
          if ( outIKey.sequence >= ikey.sequence )
            continue;
        }
        else{
          addKey();
          if ( out.fileSize() > compactionTarget_.fileSize(level+1) )
            out.reset();
        }
        outKey.clear();
        AppendInternalKey(&outKey, ikey);
        outVal << mergingIterator->value();
      }
      addKey();
      out.close();

      VersionEdit ve;
      for ( auto &f : inputs )
        ve.DeleteFile(f->level, f->number);
      for ( auto &f : createdFiles)
        ve.AddFile(f.level, f.number, f.file_size, f.smallest, f.largest);
      MutexLock l(&mutex_);
      versions_->LogAndApply( &ve, &mutex_ );
      for ( auto &f : createdFiles)
        pending_outputs_.erase(f.number);
    }
    if ( rangeDeletes_ ){
      if ( dropRD )
        rangeDeletes_->drop(move(rds));
      else
        rangeDeletes_->append(level+1, move(rds));
    }
    Log(options_.info_log,
      "Compacted level %d to %d", level, level+1 );
  }
  catch(std::exception &e){
    MutexLock l(&mutex_);
    Status s = GetStatus(e);
    if (bg_error_.ok())
      bg_error_ = s;
    Log(options_.info_log,
      "Error while compacting level %d to %d: %s", level, level+1, s.ToString().c_str() );
  }
}

size_t DBImpl::numRangeDeleteIntervals(int level)
{
    if (rangeDeletes_)
      return rangeDeletes_->numRanges(level);
    return 0;
}

DBImpl::CompactionOutput::CompactionOutput(
    std::vector<FileMetaData> *addedFilesList,
    std::set<uint64_t> *addedFileNumbers,
    const Options *o,
    port::Mutex *dbLock,
    VersionSet *versions,
    Env *env,
    int level,
    CompactionStatistics *stats
){
  this->addedFiles = addedFilesList;
  this->addedFileNumbers = addedFileNumbers;
  this->options = o;
  this->dbLock = dbLock;
  this->versions = versions;
  this->env = env;
  this->level = level;
  this->stats = stats;
}

DBImpl::CompactionOutput::~CompactionOutput()
{
  if ( table )
    abandone();
}

void DBImpl::CompactionOutput::reset()
{
  if ( table )
    close();
  decltype(FileMetaData::number) fileNum;
  {
    MutexLock l(dbLock);
    fileNum = versions->NewFileNumber();
    addedFiles->emplace_back();
    FileMetaData &f = addedFiles->back();
    f.number = fileNum;
    f.level = level;
    addedFileNumbers->insert(fileNum);
    stats->countIn( localStats );
  }
  string fname = TableFileName(*options, fileNum, level);
  WritableFile *wf;
  Status s = env->NewWritableFile(fname, &wf, gMapSize);
  EnsureOk(s);
  file.reset(wf);
  table.reset( new TableBuilder(*options, wf) );
  EnsureOk(table->status());
}

void DBImpl::CompactionOutput::add(Slice key, Slice val)
{
  table->Add(key, val);
  FileMetaData &f = addedFiles->back();
  if ( !f.smallest.valid() )
    f.smallest.DecodeFrom(key);
  f.largest.DecodeFrom(key);
  localStats.countIn(key, val);
}

uint64_t DBImpl::CompactionOutput::fileSize()
{
  return table->FileSize();
}

void DBImpl::CompactionOutput::close()
{
  if ( !table || fileSize() == 0 ){
    abandone();
    return;
  }
  EnsureOk(table->status());
  Status s;
  s = table->Finish();
  EnsureOk(s);
  FileMetaData &f = addedFiles->back();
  f.file_size = fileSize();
  assert(f.smallest.valid());
  assert(f.largest.valid());
  s = file->Sync();
  EnsureOk(s);
  s = file->Close();
  EnsureOk(s);
  table.reset();
  file.reset();
}

void DBImpl::CompactionOutput::abandone()
{
  if (!table)
    return;
  decltype(FileMetaData::number) fileNum;
  {
    MutexLock l(dbLock);
    FileMetaData &f = addedFiles->back();
    fileNum = f.number;
    addedFileNumbers->erase(fileNum);
    addedFiles->pop_back();
  }
  table->Abandon();
  table.reset();
  file.reset();
  string fname = TableFileName(*options, fileNum, level);
  env->DeleteFile(fname);
}



}  // namespace leveldb
