// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include <unordered_map>
#include <memory>
#include <functional>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/data_dictionary.h"
#include "db/version_edit.h"
#include "db/compaction_strategy.h"
#include "db/range_deletes.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/cache2.h"
#include "util/once_in.h"
#include "util/task_queue.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  virtual Status OpenFamily(const Options& options, const std::string& name);
  virtual Status CloseFamily(const std::string& name);
  // Implementations of the DB interface
  virtual Status Put(const std::string& family, const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const std::string& family, const WriteOptions&, const Slice& key);
  virtual Status Delete(const std::string& family, const WriteOptions&, const Slice& fromKey, const Slice& toKey);
  virtual Status Write(const std::string& family, const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(
      const std::string& family,
      const ReadOptions& options,
      const Slice& key,
      std::string* value);
  virtual Status Get(
      const std::string& family,
      const ReadOptions& options,
      const Slice& key,
      Value* value);

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Delete(const WriteOptions&, const Slice& fromKey, const Slice& toKey);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     Value* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  virtual void GetApproximateSizes(const Range* range, int n, uint64_t* sizes);
  /// does nothing. at all
  virtual void CompactRange(const Slice* begin, const Slice* end);
  virtual Status VerifyLevels();

  const Options & GetOptions() const { return options_; }

  // Extra methods (for testing) that are not in the public DB interface


  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  void ResizeCaches() {double_cache->ResizeCaches();};
  size_t GetCacheCapacity() {return(double_cache->GetCapacity(false));}
  void PurgeExpiredFileCache() {double_cache->PurgeExpiredFiles();};

  void BackgroundImmCompactCall();
  bool IsBackroundJobs();
  uint32_t RunningCompactionCount() {mutex_.AssertHeld(); return(running_compactions_);};

  typedef std::function<void()> OnWrite;
  void addWriteListener(OnWrite f);

  // argument - level for which compaction had just happened. stuf was just put into level+1
  typedef std::function<void(int)> OnCompactionFinished;
  void addCompactionFinishedListener(OnCompactionFinished f);
  /// decides wheither to drop the user key
  typedef std::function<bool(Slice, SequenceNumber)> DropTheKey;
  typedef std::function<std::vector<FileMetaData*>(const Version *)> GetFileList;
  /// gonna compact \a level into level+1, if one of the levels is under compaction now, the call is ignored
  void enqueueCompaction( int level );
  // called by background thread to do actual compaction
  void compact( int level );

  size_t numRangeDeleteIntervals(int level);
private:
  friend class DB;
  friend class LevelSizeCS;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit);

  // Riak routine:  pause DB::Open if too many compactions
  //  stacked up immediately.  Happens in some repairs and
  //  some Riak upgrades
  void CheckCompactionState();

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  void KeepOrDelete(const std::string & Filename, int level, const std::set<uint64_t> & Live);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable();

  Status RecoverLogFile(uint64_t log_number,
                        VersionEdit* edit,
                        SequenceNumber* max_sequence);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);

  // blocks and waits if imm_ is not yet written
  void enqueueMemWrite();
  // temporary unlocks and waits till imm write finishes.
  void waitImmWriteFinish();

  Status MakeRoomForWrite(bool force /* compact even if there is room? */);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  int64_t PrioritizeWork(bool IsLevel0);

  // initialized before options so its block_cache is available
  std::shared_ptr<DoubleCache> double_cache;

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_; // path to DB

  // table_cache_ provides its own synchronization
  TableCache* table_cache_;


  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::Mutex throttle_mutex_;   // used by write throttle to force sequential waits on callers
  port::AtomicPointer shutting_down_;

  port::CondVar bg_cv_;          // Signalled when background work finishes
  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;                // Memtable being compacted
  uint64_t logfile_number_;
  std::unique_ptr<log::Writer> log_;

  // Queue of writers.
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;


  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;   // NULL means beginning of key range
    const InternalKey* end;     // NULL means end of key range
    InternalKey tmp_storage;    // Used to keep track of compaction progress
  };

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];

  volatile uint64_t throttle_end;
  volatile uint32_t running_compactions_;

  // accessor to new, dynamic block_cache
  Cache * block_cache() {return(double_cache->GetBlockCache());};
  Cache * file_cache() {return(double_cache->GetFileCache());};

  std::string GetFamilyPath(const std::string &family_name);
  std::unordered_map< std::string, std::unique_ptr<DBImpl> > families_;
  port::RWMutex families_mtx_;

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  DBImpl(const Options& options, const std::string& dbname, std::shared_ptr<DoubleCache> cache);
  /// self destruction mechanism
  void Destruct();

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  std::vector<OnWrite> onWrite_;
  void fireOnWrite();

  std::vector<OnCompactionFinished> onCompactionFinished_;
  void fireOnCompactionFinished(int level);

  int               current_block_size_step_;    // last dynamic block size computed
  OnceIn            increaseBlockSizeInterval_;
  std::vector<bool> isCompactionSubmitted_;
  bool isCompactionSubmitted(int level);
  void compactionSubmitted(int level);
  void compactionFinished(int level);

  struct CompactionStatistics{
    uint64_t AvgKeySize = 0;
    uint64_t KeysCount  = 0;
    uint64_t AvgDataSize =0;
    
    void countIn(Slice key, Slice val){
      auto newCount = KeysCount + 1;
      AvgKeySize = (AvgKeySize * KeysCount + key.size()) / newCount;
      AvgDataSize = (AvgDataSize * KeysCount + val.size()) / newCount;
      KeysCount = newCount;
    }
    // merges two statistics. resets \a
    void countIn(CompactionStatistics &a){
      auto totalKeys = KeysCount + a.KeysCount;
      if ( totalKeys == 0 )
        return; // both empty
      AvgKeySize = (AvgKeySize * KeysCount + a.AvgKeySize * a.KeysCount) / totalKeys;
      AvgDataSize = (AvgDataSize * KeysCount + a.AvgDataSize * a.KeysCount) / totalKeys;
      KeysCount = totalKeys;
      a = CompactionStatistics();
    }
  };

  CompactionStatistics compactionStats_;

  size_t blockSize(int level);

  class CompactionOutput{
  public:
    /// will add to the lists all the created files
    CompactionOutput(
        std::vector<FileMetaData> *addedFilesList,
        std::set<uint64_t> *addedFileNumbers,
        const Options *o,
        port::Mutex *dbLock,
        VersionSet *versions,
        Env *env,
        int level,
        CompactionStatistics *stats);
    ~CompactionOutput();
    /// creates new output file. closes previous automatically
    void reset();
    void add(Slice key, Slice val);
    uint64_t fileSize();
    void close();
  private:
    std::unique_ptr<TableBuilder> table;
    std::unique_ptr<WritableFile> file;
    std::vector<FileMetaData>    *addedFiles;
    std::set<uint64_t>           *addedFileNumbers;
    const Options                *options;
    port::Mutex                  *dbLock;
    VersionSet                   *versions;
    Env                          *env;
    int                           level;
    CompactionStatistics         *stats;
    CompactionStatistics          localStats;
    void abandone();
  };

  struct CompactionTargetParams{
    uint64_t fileSize(int level) const {
      return fileSizeLimitForLevel0_ << level;
    }
    uint32_t numFilesPerLevel(int level) const {
      if (level == 0)
        return 0;
      if (level < 3)
        return 10;
      return 1000;
    }
    size_t numRangeDeletes() const{
      return numRangeDeletes_;
    }
  private:
    uint64_t fileSizeLimitForLevel0_ = 16 * 1024 * 1024;
    size_t  numRangeDeletes_ = 10000;
  };
  const CompactionTargetParams compactionTarget_;
  std::vector<std::unique_ptr<CompactionStrategy>> compactionStrategies_;

  const std::unique_ptr<RangeDeletes> rangeDeletes_;

  TaskQueue immWrite_;
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src,
                               Cache * block_cache);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
