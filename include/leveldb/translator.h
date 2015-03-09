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
    virtual size_t GetInternalKeySize(const Slice & input_key) const = 0;
    virtual void TranslateExternalKey(const Slice & input_key,
                                      char * buffer) const = 0;
    virtual void TranslateInternalKey(const Slice & internal_key,
                                      std::string * out) const = 0; 
};

class BatchTranslator {
  public:
    virtual ~BatchTranslator(){}
    virtual Status TranslateBatch(const Slice & input, WriteBatch * batch) = 0;
};

class DefaultKeyTranslator : public KeyTranslator {
  public:
    DefaultKeyTranslator() {}
    size_t GetInternalKeySize(const Slice & external_key) const;
    size_t GetExternalKeySize(const Slice & internal_key) const;
    void TranslateExternalKey(const Slice & external_key, char * buffer) const;
    void TranslateInternalKey(const Slice & internal_key,
                              std::string * out) const;
};

class TSTranslator : public KeyTranslator, public BatchTranslator {
  private:
    DataDictionary * dict_;
  public:
    TSTranslator(DataDictionary * dict);
    size_t GetInternalKeySize(const Slice & external_key) const;
    size_t GetExternalKeySize(const Slice & internal_key) const;
    void TranslateExternalKey(const Slice & input_key, char * buffer) const;
    void TranslateInternalKey(const Slice & input_key,
                              std::string * out) const;
    Status TranslateBatch(const Slice & input, WriteBatch * batch);
    static Slice GetFamilySlice(const Slice & s);
    static Slice GetSeriesSlice(const Slice & s);
};

DefaultKeyTranslator * GetDefaultKeyTranslator();
}
#endif
