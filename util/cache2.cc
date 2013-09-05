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

#include "util/cache2.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache2::~Cache2() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void* value;
  void (*deleter)(const Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
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

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
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

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
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
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
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
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;
        /*Slice key =*/ h->key();  // eliminate unused var warning, but allow for side-effects
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
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
class LRUCache2 : public Cache2 {
 public:
  LRUCache2();
  ~LRUCache2();

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }
  // Separate from constructor so caller can easily make an array of LRUCache2
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  size_t GetCapacity() const {return(capacity_);};
  size_t GetUsage() const {return(usage_);};

  // Cache2 methods to allow direct use for single shard
  virtual Cache2::Handle* Insert(const Slice& key,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value))
        {return(Insert(key, HashSlice(key), value, charge, deleter));};

  virtual Cache2::Handle* Lookup(const Slice& key)
        {return(Lookup(key, HashSlice(key)));};

  virtual void Release(Cache2::Handle* handle);
  virtual void Erase(const Slice& key)
       {Erase(key, HashSlice(key));};
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  virtual uint64_t NewId() {
    return (++last_id_);
  }

  virtual size_t EntryOverheadSize() {return(sizeof(LRUHandle));};

  // Like Cache2 methods, but with an extra "hash" parameter.
  Cache2::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache2::Handle* Lookup(const Slice& key, uint32_t hash);

  void Erase(const Slice& key, uint32_t hash);

    virtual void Addref(Cache2::Handle* handle);

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.
  size_t capacity_;

  // mutex_ protects the following state.
  port::Spin spin_;
  size_t usage_;
  uint64_t last_id_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  LRUHandle lru_;

  HandleTable table_;
};

LRUCache2::LRUCache2()
    : usage_(0),
      last_id_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

LRUCache2::~LRUCache2() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;

    // must allow for 2 refs given overlapped files
    //  being double ref'd intentionally ... ruins this assert :-(
    assert(e->refs == 1 || e->refs == 2);  // Error if caller has an unreleased handle
    Unref(e);
    e = next;
  }
}

void LRUCache2::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

void LRUCache2::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache2::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache2::Handle* LRUCache2::Lookup(const Slice& key, uint32_t hash) {
  SpinLock l(&spin_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++;
    LRU_Remove(e);
    LRU_Append(e);
  }
  return reinterpret_cast<Cache2::Handle*>(e);
}

void LRUCache2::Release(Cache2::Handle* handle) {
  SpinLock l(&spin_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

void LRUCache2::Addref(Cache2::Handle* handle) {
  SpinLock l(&spin_);
  LRUHandle * e;

  e=reinterpret_cast<LRUHandle*>(handle);
  if (NULL!=e && 1 <= e->refs)
      ++e->refs;
}

Cache2::Handle* LRUCache2::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  SpinLock l(&spin_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache2, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  LRU_Append(e);
  usage_ += charge;

  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old);
    Unref(old);
  }


  // Riak - matthewv: code added to remove old only if it was not active.
  //  Had scenarios where file cache would be largely or totally drained
  //  because an active object does NOT reduce usage_ upon delete.  So
  //  the previous while loop would basically delete everything.
  LRUHandle * next, * cursor;

  for (cursor=lru_.next; usage_ > capacity_ && cursor != &lru_; cursor=next)
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

  return reinterpret_cast<Cache2::Handle*>(e);
}

void LRUCache2::Erase(const Slice& key, uint32_t hash) {
  SpinLock l(&spin_);
  LRUHandle* e = table_.Remove(key, hash);
  if (e != NULL) {
    LRU_Remove(e);
    Unref(e);
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache2 : public Cache2 {
 private:
  LRUCache2 shard_[kNumShards];
  port::Spin id_spin_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache2(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache2() { }
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
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Addref(handle);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    SpinLock l(&id_spin_);
    return ++(last_id_);
  }
  virtual size_t EntryOverheadSize() {return(sizeof(LRUHandle));};
};

}  // end anonymous namespace


}  // namespace leveldb
