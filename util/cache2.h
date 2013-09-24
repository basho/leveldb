// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

//
// mildly modified version of Google's original cache.cc to support
//  Riak's flexcache.cc
//

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE2_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE2_H_

#include <stdint.h>
#include "leveldb/cache.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "util/flexcache.h"

namespace leveldb {

class ShardedLRUCache2;

/**
 * DoubleCache holds the file cache and the block cache to easy
 *  interactive sizing
 */

class DoubleCache
{
public:
    explicit DoubleCache(const Options & options);
    virtual ~DoubleCache();

    Cache * GetFileCache() {return((Cache *)m_FileCache);};
    Cache * GetBlockCache() {return((Cache *)m_BlockCache);};

    void ResizeCaches();
    size_t GetCapacity(bool IsFileCache);
    time_t GetFileTimeout() {return(m_FileTimeout);};
    void SetFileTimeout(time_t Timeout) {m_FileTimeout=Timeout;};

    void Flush();
    void PurgeExpiredFiles();

protected:
    ShardedLRUCache2 * m_FileCache;   //!< file cache used by db/tablecache.cc
    ShardedLRUCache2 * m_BlockCache;  //!< used by table/table.cc

    bool m_IsInternalDB;        //!< internal db gets smaller allocation from FlexCache
    size_t m_Overhead;          //!< reduce from allocation to better estimate limits
    size_t m_TotalAllocation;
    time_t m_FileTimeout;       //!< seconds to allow file to stay cached.  default 4 days.

private:
    DoubleCache();                       //!< no default constructor
    DoubleCache(const DoubleCache &);    //!< no copy constructor
    void operator=(const DoubleCache &); //!< no assignment

};  // class DoubleCache




}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_CACHE2_H_
