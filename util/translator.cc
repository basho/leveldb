// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/translator.h"
#include "leveldb/write_batch.h"
#include "db/data_dictionary.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace leveldb {

size_t DefaultKeyTranslator::GetInternalKeySize(const Slice & user_key) const {
  return user_key.size();
}

void DefaultKeyTranslator::TranslateExternalKey(const Slice & user_key,
                                   char * buffer) const {
  memcpy(buffer, user_key.data(), user_key.size());
}

void DefaultKeyTranslator::TranslateInternalKey(const Slice & internal_key,
                                                std::string * out) const {
  out->append(internal_key.data(), internal_key.size());
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

size_t TSTranslator::GetInternalKeySize(const Slice & user_key) const {
  return 8 + 4 + 4;
}

void TSTranslator::TranslateExternalKey(const Slice & user_key, char * buffer) const {
  // Copy 64 bit timestamp, 32 bit family id, 32 bit series id
  memcpy(buffer, user_key.data(), 8);
  // TODO: Do in a batch to avoid locking twice for new data.
  uint32_t family_id = dict_->ToId(GetFamilySlice(user_key));
  uint32_t series_id = dict_->ToId(GetSeriesSlice(user_key));
  EncodeFixed32(buffer + 8, family_id);
  EncodeFixed32(buffer + 8 + 4, series_id);
}

/*
 * Converts internal key with fields:
 * - 64 bit timestamp
 * - 32 bit family id
 * - 32 bit series id
 * to external key with 
 * - 64 bit timestamp
 * - length prefixed family string
 * - length prefixed series string
 */
void TSTranslator::TranslateInternalKey(const Slice & internal_key,
                                        std::string * out) const {
  Slice input = internal_key;
  // Append 64 bit timestamp as is
  out->append(input.data(), 8);
  input.remove_prefix(8);
  uint32_t family_id, series_id;
  GetFixed32(&input, &family_id);
  GetFixed32(&input, &series_id);
  Slice family, series;
  dict_->ToString(family_id, &family);
  dict_->ToString(series_id, &series);
  PutLengthPrefixedSlice(out, family);
  PutLengthPrefixedSlice(out, series);
}

Status TSTranslator::TranslateBatch(const Slice & input_bin,
                                    WriteBatch * write_batch) {
  /*
   * Format:
   * Table : string
   * Series : string
   * [Time : uint64, Data : string]
   */
  Slice input = input_bin;
  std::string key_buffer;

  Slice family;
  if (!GetLengthPrefixedSlice(&input, &family))
    return Status::Corruption("Bad family name in TS batch");

  Slice name;
  if (!GetLengthPrefixedSlice(&input, &name))
    return Status::Corruption("Bad series name in TS batch");

  uint32_t family_id = dict_->ToId(family);
  uint32_t series_id = dict_->ToId(name);

  while(input.size() > 8) {
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
