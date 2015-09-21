// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include "leveldb/perf_count.h"

namespace leveldb {

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  if (NULL!=tf->doublecache)
      tf->doublecache->SubFileSize(tf->table->GetFileSize());
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       Cache * file_cache,
                       DoubleCache & doublecache)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(file_cache),
      doublecache_(doublecache)
{
}

TableCache::~TableCache() {
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, int level,
                             Cache::Handle** handle, bool is_compaction) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(*options_, file_number, level);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);

      // Riak:  support opportunity to manage Linux page cache
      if (is_compaction)
          file->SetForCompaction(file_size);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      tf->doublecache = &doublecache_;
      tf->file_number = file_number;
      tf->level = level;

      *handle = cache_->Insert(key, tf, table->TableObjectSize(), &DeleteEntry);
      gPerfCounters->Inc(ePerfTableOpened);
      doublecache_.AddFileSize(table->GetFileSize());

      // temporary hardcoding to match number of levels defined as
      //  overlapped in version_set.cc
      if (level<config::kNumOverlapLevels)
          cache_->Addref(*handle);
    }
  }
  else
  {
      // for Linux, let fadvise start precaching
      if (is_compaction)
      {
          RandomAccessFile *file = reinterpret_cast<TableAndFile*>(cache_->Value(*handle))->file;
          file->SetForCompaction(file_size);
      }   // if

      gPerfCounters->Inc(ePerfTableCached);
  }   // else
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  int level,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, level, &handle, options.IsCompaction());

  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       int level,
                       const Slice& k,
                       void* arg,
                       bool (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, level, &handle);

  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number, bool is_overlapped) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);

  // overlapped files have extra reference to prevent their purge,
  //  release that reference now
  if (is_overlapped)
  {
      Cache::Handle *handle;

      // the Lookup call adds a reference too, back out both
      handle=cache_->Lookup(Slice(buf, sizeof(buf)));

      // with multiple background threads, file might already be
      //  evicted
      if (NULL!=handle)
      {
          cache_->Release(handle);  // release for Lookup() call just made
          cache_->Release(handle);  // release for extra reference
      }   // if
  }   // if

  cache_->Erase(Slice(buf, sizeof(buf)));
}

/**
 * Riak specific routine to return table statistic ONLY if table metadata
 *  already within cache ... otherwise return 0.
 */
uint64_t
TableCache::GetStatisticValue(
    uint64_t file_number,
    unsigned Index)
{
    uint64_t ret_val;
    char buf[sizeof(file_number)];
    Cache::Handle *handle;

    ret_val=0;
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    handle = cache_->Lookup(key);

    if (NULL != handle)
    {
        TableAndFile * tf;

        tf=reinterpret_cast<TableAndFile*>(cache_->Value(handle));
        ret_val=tf->table->GetSstCounters().Value(Index);
        cache_->Release(handle);
    }   // if

    return(ret_val);

}   // TableCache::GetStatisticValue


/**
 * Riak specific routine to push list of open files to disk
 */
void
TableCache::SaveOpenFileList()
{
    Status s;
    std::string cow_name;
    WritableFile * cow_file;
    log::Writer * cow_log;

    cow_name=CowFileName(dbname_);
    s = env_->NewWritableFile(cow_name, &cow_file, 4*1024L);
    if (s.ok())
    {
        std::string buffer;
        size_t file_count;

        buffer.reserve(4*1024L);
        cow_log=new log::Writer(cow_file);
        file_count=doublecache_.WriteCacheObjectWarming(buffer);
        s = cow_log->AddRecord(buffer);
        delete cow_log;
        delete cow_file;

        if (s.ok())
        {
            Log(options_->info_log, "Wrote %zd file cache objects for warming.",
                file_count);
        }   // if
        else
        {
            Log(options_->info_log, "Error writing cache object file %s (error %s)",
                cow_name.c_str(), s.ToString().c_str());
            env_->DeleteFile(cow_name);
        }   // else
    }   // if
    else
    {
        Log(options_->info_log, "Unable to create cache object file %s (error %s)",
            cow_name.c_str(), s.ToString().c_str());
    }   // else

    return;

}   // TableCache::SaveOpenFiles

/**
 * Riak specific routine to read list of previously open files
 *  and preload them into the table cache
 */
void
TableCache::PreloadTableCache()
{
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;
        virtual void Corruption(size_t bytes, const Status& s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                (this->status == NULL ? "(ignoring error) " : ""),
                fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != NULL && this->status->ok()) *this->status = s;
        }
    };

    Status s;
    std::string cow_name;
    SequentialFile * cow_file;
    log::Reader * cow_log;
    size_t obj_count(0);

    cow_name=CowFileName(dbname_);
    s = env_->NewSequentialFile(cow_name, &cow_file);
    if (s.ok())
    {
        // Create the log reader.
        LogReporter reporter;
        reporter.env = env_;
        reporter.info_log = options_->info_log;
        reporter.fname = cow_name.c_str();
        reporter.status = &s;

        std::string buffer;
        Slice record;

        cow_log=new log::Reader(cow_file, &reporter, true, 0);

        while (cow_log->ReadRecord(&record, &buffer) && s.ok())
        {
            Slice input = record;
            uint32_t tag, level;
            uint64_t file_no, file_size;
            Cache::Handle * handle;
            Status s2;

            // the on disk format is created in WriteFileCacheObjectWarming()
            //  (util/cache2.cc)
            while (GetVarint32(&input, &tag))
            {
                if (VersionEdit::kFileCacheObject==tag)
                {
                  GetVarint32(&input, &level);
                  GetVarint64(&input, &file_no);
                  GetVarint64(&input, &file_size);

                  // do not care if this succeeds, but need status
                  //  for handle maintenance
                  handle=NULL;

                  // set compaction flag to suggest Linux start pre-reading the files
                  s2=FindTable(file_no, file_size, level, &handle, (level<config::kNumOverlapLevels));

                  if (s2.ok())
                  {
                      cache_->Release(handle);
                      handle=NULL;
                      ++obj_count;
                  }   // if
                }   // if
                else
                {
                    Log(options_->info_log,"Unknown tag (%u) seen in preload file %s",
                        tag, cow_name.c_str());
                }   // else
            }   // while
      }   // while

      delete cow_log;
      delete cow_file;

      // delete the physical file at this point
      //   (keep bad file for possible review?)
      env_->DeleteFile(cow_name);

      Log(options_->info_log, "File cache warmed with %zd files.", obj_count);
  }   // if
  else
  {
      Log(options_->info_log, "No cache warming file detected.");
  }   // else

}   // TableCache::PreloadTableCache

}  // namespace leveldb
