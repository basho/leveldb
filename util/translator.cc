// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/translator.h"
#include "leveldb/write_batch.h"
#include "db/data_dictionary.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace leveldb {

size_t DefaultKeyTranslator::GetOutputSize(const Slice & user_key) const {
  return user_key.size();
}

void DefaultKeyTranslator::TranslateKey(const Slice & user_key,
                                   char * buffer) const {
  memcpy(buffer, user_key.data(), user_key.size());
}

static port::OnceType once = LEVELDB_ONCE_INIT;
static DefaultKeyTranslator * default_translator = NULL;

static void InitDefaultKeyTranslator() {
  default_translator = new DefaultKeyTranslator;
}

DefaultKeyTranslator * GetDefaultKeyTranslator() {
  port::InitOnce(&once, InitDefaultKeyTranslator);
  return default_translator;
}

// Time series data translator.
TSTranslator::TSTranslator(DataDictionary * dd) : dict_(dd) {}

size_t TSTranslator::GetOutputSize(const Slice & user_key) const {
  return 8 + 4 + 4;
}

void TSTranslator::TranslateKey(const Slice & user_key, char * buffer) const {
  // Copy 64 bit timestamp, 32 bit family id, 32 bit series id
  memcpy(buffer, user_key.data(), 8);
  // TODO: Do in a batch to avoid locking twice for new data.
  uint32_t family_id = dict_->ToId(GetFamilySlice(user_key));
  uint32_t series_id = dict_->ToId(GetSeriesSlice(user_key));
  EncodeFixed32(buffer + 8, family_id);
  EncodeFixed32(buffer + 8 + 4, series_id);
}

Status TSTranslator::TranslateBatch(const Slice & input_bin,
                                    WriteBatch * write_batch) {
  Slice input = input_bin;
  /*
   number of series: varint32
   series 1 name length: varint32
   series 1 name
   number  of points: varint32
      timestamp: 64 bits
      value: prefixed bytes
   */
  uint32_t num_series;
  if (!GetVarint32(&input, &num_series)) {
      return Status::Corruption("Bad number of series in TS batch");
  }
  std::string key_buffer;

  for (uint32_t s = num_series; s > 0; --s) {
      Slice family;
      if (!GetLengthPrefixedSlice(&input, &family))
          return Status::Corruption("Bad family name in TS batch");

      Slice name;
      if (!GetLengthPrefixedSlice(&input, &name))
          return Status::Corruption("Bad series name in TS batch");

      uint32_t num_points;

      if (!GetVarint32(&input, &num_points))
          return Status::Corruption("Bad number of points in TS batch");

      uint32_t family_id = dict_->ToId(family);
      uint32_t series_id = dict_->ToId(name);

      for (uint32_t p = num_points; p > 0; --p) {
          uint64_t timestamp;
          if (!GetFixed64(&input, &timestamp)) {
              return Status::Corruption("Bad timestamp in TS batch");
          }
          Slice value;
          if (!GetLengthPrefixedSlice(&input, &value)) {
              return Status::Corruption("Bad value in TS batch");
          }
          key_buffer.resize(0);
          PutFixed64(&key_buffer, timestamp);
          PutFixed32(&key_buffer, family_id);
          PutFixed32(&key_buffer, series_id);
          Slice key_slice(key_buffer);
          write_batch->Put(key_slice, value);
      } // For each point

  } // For each series
  return Status::OK();
}

Slice TSTranslator::GetFamilySlice(const Slice & s) {
  return GetLengthPrefixedSlice((const char *)s.data() + 8);
}

Slice TSTranslator::GetSeriesSlice(const Slice & s) {
  Slice fam_slice = GetFamilySlice(s);
  return GetLengthPrefixedSlice(fam_slice.data() + fam_slice.size());
}

} // namespace leveldb
