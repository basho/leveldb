// Copyright (c) 2015  Basho Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/data_dictionary.h"
#include "util/coding.h"

namespace leveldb {
  
bool DataDictionary::ToString(uint32_t id, Slice * out_str) const {
  Id2NameTable::Iterator it(&id2name_);
  it.Seek(id);
  if (it.Valid() && Id2NameComparator()(id, it.key()) == 0) {
    *out_str = GetLengthPrefixedSlice(it.key() + 4);
    return true;
  } else {
    return false;
  }
}

bool DataDictionary::ToId(const Slice & name, uint32_t * id_out) const {
  Name2IdTable::Iterator it(&name2id_);
  LookupKey lookup_key(name);
  it.Seek(lookup_key);
  if (it.Valid() && Name2IdComparator()(lookup_key, it.key()) == 0) {
    *id_out = DecodeFixed32(it.key());
    return true;
  } else {
    return false;
  }
}

uint32_t DataDictionary::ToId(const Slice & s) {
  uint32_t id;
  if (!ToId(s, &id)) {
    // Lock, try to insert, read again.
    mutex_.Lock();
    if (!ToId(s, &id)) {
      const size_t encoded_len = 4 + VarintLength(s.size()) + s.size();
      char * buf = arena_.Allocate(encoded_len);
      id = id_++;
      EncodeFixed32(buf, id);
      char * p = buf + 4;
      p = EncodeVarint32(p, s.size());
      memcpy(p, s.data(), s.size());
      name2id_.Insert(buf);
      id2name_.Insert(buf);
    }
    mutex_.Unlock();
  }
  return id;
}

int DataDictionary::Name2IdComparator::operator()(const char * a,
                                                  const char * b) const {
  Slice a_slice = GetLengthPrefixedSlice(a + 4);
  Slice b_slice = GetLengthPrefixedSlice(b + 4);
  return a_slice.compare(b_slice);
}

int DataDictionary::Name2IdComparator::operator()(const char * a,
                                                  const DataDictionary::LookupKey & b) const {
  Slice a_slice = GetLengthPrefixedSlice(a + 4);
  Slice b_slice = b.slice;
  return a_slice.compare(b_slice);
}

int DataDictionary::Name2IdComparator::operator()(const DataDictionary::LookupKey & a,
                                                  const char * b) const {
  Slice a_slice = a.slice;
  Slice b_slice = GetLengthPrefixedSlice(b + 4);
  return a_slice.compare(b_slice);
}

int DataDictionary::Id2NameComparator::operator() (const char * a,
                                                   const char * b) const {
  Slice a_slice(a, 4), b_slice(b, 4);
  return a_slice.compare(b_slice);
}

int DataDictionary::Id2NameComparator::operator() (uint32_t a,
                                                   const char * b) const {
  if (port::kLittleEndian) {
    Slice a_slice((const char*)&a, 4);
    Slice b_slice(b, 4);
    return a_slice.compare(b_slice);
  } else {
    char abuf[4];
    EncodeFixed32(abuf, a);
    Slice a_slice(abuf, 4);
    Slice b_slice(b, 4);
    return a_slice.compare(b_slice);
  }
}

int DataDictionary::Id2NameComparator::operator() (const char * a,
                                                   uint32_t b) const {
  if (port::kLittleEndian) {
    Slice a_slice(a, 4);
    Slice b_slice((const char*)&b, 4);
    return a_slice.compare(b_slice);
  } else {
    Slice a_slice(a, 4);
    char b_buf[4];
    EncodeFixed32(b_buf, b);
    Slice b_slice(b_buf, 4);
    return a_slice.compare(b_slice);
  }
}

} // leveldb namespace
