// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

//
// mildly modified version of Google's original cache.cc to support
//  Riak's flexcache.cc
//


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/atomics.h"
#include "util/cache2.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle2 {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle2* next_hash;
  LRUHandle2* next;
  LRUHandle2* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle2* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle2* Insert(LRUHandle2* h) {
    LRUHandle2** ptr = FindPointer(h->key(), h->hash);
    LRUHandle2* old = *ptr;
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle2* Remove(const Slice& key, uint32_t hash) {
    LRUHandle2** ptr = FindPointer(key, hash);
    LRUHandle2* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle2** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle2** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle2** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle2** new_list = new LRUHandle2*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle2* h = list_[i];
      while (h != NULL) {
        LRUHandle2* next = h->next_hash;
        /*Slice key =*/ h->key();  // eliminate unused var warning, but allow for side-effects
        uint32_t hash = h->hash;
        LRUHandle2** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};


// A single shard of sharded cache.
class LRUCache2 : public Cache {
 public:
  LRUCache2();
  ~LRUCache2();

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }
  // Separate from constructor so caller can easily make an array of LRUCache2

  // Cache2 methods to allow direct use for single shard
  virtual Cache::Handle* Insert(const Slice& key,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value))
        {return(Insert(key, HashSlice(key), value, charge, deleter));};

  virtual Cache::Handle* Lookup(const Slice& key)
        {return(Lookup(key, HashSlice(key)));};

  virtual void Release(Cache::Handle* handle);
  virtual bool ReleaseOne();
  virtual void Erase(const Slice& key)
       {Erase(key, HashSlice(key));};
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle2*>(handle)->value;
  }

  virtual uint64_t NewId() {
      return inc_and_fetch(&last_id_);
  }

  virtual size_t EntryOverheadSize() {return(sizeof(LRUHandle2));};

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);

  void Erase(const Slice& key, uint32_t hash);

  virtual void Addref(Cache::Handle* handle);

  void SetParent(ShardedLRUCache2 * Parent, bool IsFileCache)
    {parent_=Parent; is_file_cache_=IsFileCache;};

 private:
  void LRU_Remove(LRUHandle2* e);
  void LRU_Append(LRUHandle2* e);
  void Unref(LRUHandle2* e);

  // Initialized before use.
  class ShardedLRUCache2 * parent_;
  bool is_file_cache_;

  // mutex_ protects the following state.
  port::Spin spin_;
  uint64_t last_id_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle2 lru_;

  HandleTable table_;
};

LRUCache2::LRUCache2()
  : parent_(NULL), is_file_cache_(true), last_id_(0)
{
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache2::~LRUCache2() {
  for (LRUHandle2* e = lru_.next; e != &lru_; ) {
    LRUHandle2* next = e->next;

    assert(e->refs == 1);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache2::LRU_Remove(LRUHandle2* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache2::LRU_Append(LRUHandle2* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache2::Lookup(const Slice& key, uint32_t hash) {
  SpinLock l(&spin_);
  LRUHandle2* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache2::Release(Cache::Handle* handle) {
  SpinLock l(&spin_);
  Unref(reinterpret_cast<LRUHandle2*>(handle));
}

void LRUCache2::Addref(Cache::Handle* handle) {
  SpinLock l(&spin_);
  LRUHandle2 * e;

  e=reinterpret_cast<LRUHandle2*>(handle);
  if (NULL!=e && 1 <= e->refs)
      ++e->refs;
}


void LRUCache2::Erase(const Slice& key, uint32_t hash) {
  SpinLock l(&spin_);
  LRUHandle2* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

}  // end anonymous namespace


static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache2 : public Cache {
public:
  volatile uint64_t usage_;        // cache2's usage is across all shards,
                                   //  simplifies FlexCache management

private:
  LRUCache2 shard_[kNumShards];
  port::Spin id_spin_;
  DoubleCache & parent_;
  bool is_file_cache_;
  size_t next_shard_;
  volatile uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache2(class DoubleCache & Parent, bool IsFileCache)
      : usage_(0), parent_(Parent), is_file_cache_(IsFileCache), next_shard_(0), last_id_(0) {
    for (int s = 0; s < kNumShards; s++)
    {
        shard_[s].SetParent(this, IsFileCache);
    }

  }
  virtual ~ShardedLRUCache2() { }
  volatile uint64_t GetUsage() const {return(usage_);};
  volatile uint64_t * GetUsagePtr() {return(&usage_);};
  volatile uint64_t GetCapacity() {return(parent_.GetCapacity(is_file_cache_));}

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Addref(Handle* handle) {
    LRUHandle2* h = reinterpret_cast<LRUHandle2*>(handle);
    shard_[Shard(h->hash)].Addref(handle);
  }
  virtual void Release(Handle* handle) {
    LRUHandle2* h = reinterpret_cast<LRUHandle2*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle2*>(handle)->value;
  }
  virtual uint64_t NewId() {
      return inc_and_fetch(&last_id_);
  }
  virtual size_t EntryOverheadSize() {return(sizeof(LRUHandle2));};

  // reduce usage of all shards to fit within current capacity limit
  void Resize()
  {
      size_t end_shard;
      bool one_deleted;

      SpinLock l(&id_spin_);
      end_shard=next_shard_;
      one_deleted=true;

      while((parent_.GetCapacity(is_file_cache_) < usage_) && one_deleted)
      {
          one_deleted=false;

          // round robin delete ... later, could delete from most full or such
          //   but keep simple since using spin lock
          do
          {
              one_deleted=shard_[next_shard_].ReleaseOne();
              next_shard_=(next_shard_ +1) % kNumShards;
          } while(end_shard!=next_shard_ && !one_deleted);
      }   // while

      return;

  } // ShardedLRUCache2::Resize

};  //ShardedLRUCache2


/**
 * Initialize cache pair based upon current conditions
 */
DoubleCache::DoubleCache(
    bool IsInternalDB)
    : m_IsInternalDB(IsInternalDB)
{
    m_FileCache=new ShardedLRUCache2(*this, true);
    m_BlockCache=new ShardedLRUCache2(*this, false);

    m_TotalAllocation=gFlexCache.GetDBCacheCapacity(IsInternalDB);

}   // DoubleCache::DoubleCache


DoubleCache::~DoubleCache()
{
    delete m_FileCache;
    delete m_BlockCache;

}   // DoubleCache::DoubleCache


/**
 * Resize each of the caches based upon new global conditions
 */
void
DoubleCache::ResizeCaches()
{
    // worst case is size reduction, take from block cache first
    m_TotalAllocation=gFlexCache.GetDBCacheCapacity(m_IsInternalDB);
    m_BlockCache->Resize();
    m_FileCache->Resize();

    return;

}   // DoubleCache::ResizeCaches()


/**
 * Calculate limit to file or block cache based upon global conditions
 */
size_t
DoubleCache::GetCapacity(
    bool IsFileCache)
{
    size_t  ret_val;

    ret_val=0;

    // file capacity is "fixed", it is always the entire
    //  cache allocation less minimum block size
    if (IsFileCache)
    {
        ret_val=m_TotalAllocation - (2*1024*1024);
    }   // if

    // block cache capacity is whatever file cache is not
    //  not using, or its minimum ... whichever is larger
    else
    {
        ret_val=m_TotalAllocation - m_FileCache->GetUsage();
        if (ret_val < (2*1024*1024))
            ret_val=(2*1024*1024);
    }   // else

    // fixed allocation for recovery log and info LOG: 20M each
    //  (with 64 or open databases, this is a serious number)
    if (40*1024*1024L < ret_val)
        ret_val-=40*1024*1024L;
    else
        ret_val=0;

    return(ret_val);

}   // DoubleCache::GetCapacity


//
// Definitions moved so they could access ShardedLRUCache members
//  (subtle hint to Google that every object should have .h file
//    because future reuse is unknowable)
//
void LRUCache2::Unref(LRUHandle2* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
      sub_and_fetch(parent_->GetUsagePtr(), (uint64_t)e->charge);
//    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}


Cache::Handle* LRUCache2::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  SpinLock l(&spin_);

  LRUHandle2* e = reinterpret_cast<LRUHandle2*>(
      malloc(sizeof(LRUHandle2)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache2, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e);
  add_and_fetch(parent_->GetUsagePtr(), (uint64_t)charge);
//  usage_ += charge;

  LRUHandle2* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }

  // Riak - matthewv: code added to remove old only if it was not active.
  //  Had scenarios where file cache would be largely or totally drained
  //  because an active object does NOT reduce usage_ upon delete.  So
  //  the previous while loop would basically delete everything.
  LRUHandle2 * next, * cursor;

  for (cursor=lru_.next; parent_->GetUsage() > parent_->GetCapacity() && cursor != &lru_; cursor=next)
  {
      // take next pointer before potentially destroying cursor
      next=cursor->next;

      // only delete cursor if it will actually destruct and
      //   return value to usage_
      if (cursor->refs <= 1)
      {
          LRU_Remove(cursor);
          table_.Remove(cursor->key(), cursor->hash);
          Unref(cursor);
      }   // if
  }   // for

  return reinterpret_cast<Cache::Handle*>(e);
}


bool
LRUCache2::ReleaseOne()
{
    bool ret_flag;
    LRUHandle2 * next, * cursor;
    SpinLock lock(&spin_);

    ret_flag=false;

    for (cursor=lru_.next; !ret_flag && parent_->GetUsage() > parent_->GetCapacity() && cursor != &lru_; cursor=next)
    {
        // take next pointer before potentially destroying cursor
        next=cursor->next;

        // only delete cursor if it will actually destruct and
        //   return value to usage_
        if (cursor->refs <= 1)
        {
            LRU_Remove(cursor);
            table_.Remove(cursor->key(), cursor->hash);
            Unref(cursor);
            ret_flag=true;
        }   // if
    }   // for

    return(ret_flag);

}   // LRUCache2::ReleaseOne

}  // namespace leveldb

