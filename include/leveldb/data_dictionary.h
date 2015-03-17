// Copyright (c) 2015 Basho Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_DATA_DICTIONARY_H_
#define LEVELDB_DATA_DICTIONARY_H_

#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class DataDictionaryImpl;

class DataDictionary {
  public:
    DataDictionary(const std::string & basedir);
    ~DataDictionary();
    bool ToString(uint32_t id, Slice * out) const;
    uint32_t ToId(const Slice & name);
    Status GetLoadStatus() const;
  private:
    DataDictionaryImpl * impl_;
};

}

#endif
