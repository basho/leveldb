// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// We recover the contents of the descriptor from the other files we find.
// (1) Any log files are first converted to tables
// (2) We scan every table to compute
//     (a) smallest/largest for the table
//     (b) largest sequence number in the table
// (3) We generate descriptor contents:
//      - log number is set to zero
//      - next-file-number is set to 1 + largest file number we found
//      - last-sequence-number is set to largest sequence# found across
//        all tables (see 2c)
//      - compaction pointers are cleared
//      - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

#include "db/builder.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "util/mutexlock.h"
#include <fstream>

namespace leveldb {

namespace {

class Repairer {
 public:
  Repairer(const std::string& dbname, const Options& options)
      : dbname_(dbname),
        env_(options.env),
        icmp_(options.comparator),
        ipolicy_(options.filter_policy),
        options_(SanitizeOptions(dbname, &icmp_, &ipolicy_, options)),
        owns_info_log_(options_.info_log != options.info_log),
        owns_cache_(options_.block_cache != options.block_cache),
        has_level_dirs_(false),
        db_lock_(NULL),
        next_file_number_(1) {
    // TableCache can be small since we expect each table to be opened once.
    table_cache_ = new TableCache(dbname_, &options_, 10);
    versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             &icmp_);
  }

  ~Repairer() {
    delete table_cache_;
    delete versions_;
    if (owns_info_log_) {
      delete options_.info_log;
    }
    if (owns_cache_) {
      delete options_.block_cache;
    }
  }

  Status Run() {
    Status status;
    status = env_->LockFile(LockFileName(dbname_), &db_lock_);

    if (status.ok())
        status = MakeLevelDirectories(env_, dbname_);

    std::string c = dbname_ + "/sst.txt";
    std::ifstream infile(c.c_str());

    if (status.ok()) {
          port::Mutex mu;
          MutexLock l(&mu);
          uint64_t fnum;
          int lnum;
          VersionEdit edit;
          status = versions_->Recover();
          while (infile >> lnum >> fnum) {
              edit.DeleteFile(lnum, fnum);
              Log(options_.info_log, "Deleting #%lld\n",
                static_cast<unsigned long long>(fnum));
          }
          if (status.ok()) {
              status = versions_->LogAndApply(&edit, &mu);
              Log(options_.info_log, "Extra special manifest repair is complete.");
          } 
      if (db_lock_ != NULL) {
        env_->UnlockFile(db_lock_);
      }
    }
    return status;
  }

  std::string const dbname_;
  Env* const env_;
  InternalKeyComparator const icmp_;
  InternalFilterPolicy const ipolicy_;
  Options const options_;
  bool owns_info_log_;
  bool owns_cache_;
  bool has_level_dirs_;
  FileLock* db_lock_;
  TableCache* table_cache_;
  VersionSet* versions_;
  uint64_t next_file_number_;
};
}  // namespace

Status RepairDB(const std::string& dbname, const Options& options) {
  Repairer repairer(dbname, options);
  return repairer.Run();
}

}  // namespace leveldb
