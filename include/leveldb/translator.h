// Copyright (c) 2015 Basho Techonologies. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_TRANSLATOR_H_
#define LEVELDB_TRANSLATOR_H_

#include "leveldb/translator.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb/status.h"

namespace leveldb {

class DataDictionary;
class DB;

class KeyTranslator {
  public:
    virtual ~KeyTranslator(){}
    virtual size_t GetOutputSize(const Slice & user_key) const = 0;
    virtual void TranslateKey(const Slice & user_key, char * buffer) const = 0;
};

class BatchTranslator {
  public:
    virtual ~BatchTranslator(){}
    virtual Status TranslateBatch(const Slice & input, WriteBatch * batch) = 0;
};

class DefaultKeyTranslator : public KeyTranslator {
  public:
    DefaultKeyTranslator() {}
    size_t GetOutputSize(const Slice & user_key) const;
    void TranslateKey(const Slice & user_key, char * buffer) const;
};

class TSTranslator : public KeyTranslator, public BatchTranslator {
  private:
    DataDictionary * dict_;
  public:
    TSTranslator(DataDictionary * dict);
    size_t GetOutputSize(const Slice & user_key) const;
    void TranslateKey(const Slice & user_key, char * buffer) const;
    Status TranslateBatch(const Slice & input, WriteBatch * batch);
    static Slice GetFamilySlice(const Slice & s);
    static Slice GetSeriesSlice(const Slice & s);
};

DefaultKeyTranslator * GetDefaultKeyTranslator();
}
#endif
