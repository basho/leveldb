// Copyright (c) 2015 Basho Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_DATA_DICTIONARY_H_
#define LEVELDB_DATA_DICTIONARY_H_

#include "db/skiplist.h"
#include "util/arena.h"
#include "leveldb/slice.h"

namespace leveldb {

class DataDictionary {
  public:
    //DataDictionary(const Options & options, const std::string & dbname);
    DataDictionary() : id_(0),
    name2id_(Name2IdComparator(), &arena_),
    id2name_(Id2NameComparator(), &arena_) {}

    bool ToString(uint32_t id, Slice * out) const;
    uint32_t ToId(const Slice & name);

  private:

    struct LookupKey {
      const Slice slice;
      LookupKey(const Slice & s) : slice(s) {}
    };

    bool ToId(const Slice & name, uint32_t * id) const;

    struct Name2IdComparator {
      int operator()(const char * a, const char * b) const;
      int operator()(const LookupKey & a, const char * b) const;
      int operator()(const char * a, const LookupKey & b) const;
    };

    struct Id2NameComparator {
      int operator()(const char * a, const char * b) const;
      int operator()(uint32_t a, const char * b) const;
      int operator()(const char * a, uint32_t b) const;
    };

    typedef SkipList<const char*, Name2IdComparator> Name2IdTable;
    typedef SkipList<const char*, Id2NameComparator> Id2NameTable;

    uint32_t id_;
    Arena arena_;
    port::Mutex mutex_;
    Name2IdTable name2id_;
    Id2NameTable id2name_;
};

}

#endif
