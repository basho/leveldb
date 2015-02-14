// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"
#include "db/data_dictionary.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}


Status convert_ts_key(DB * db, const Slice & in_key, std::string * out_key)
{
    DataDictionary * dict = db->GetDataDictionary();
    Slice input = in_key;
    input.remove_prefix(8);

    Slice family;
    if (!GetLengthPrefixedSlice(&input, &family))
      return Status::Corruption("Bad family name in TS batch");

    Slice name;
    if (!GetLengthPrefixedSlice(&input, &name))
      return Status::Corruption("Bad series name in TS batch");

    uint32_t family_id = dict->ToId(family);
    uint32_t series_id = dict->ToId(name);

    out_key->resize(0);
    out_key->append(in_key.data(), 8);
    PutFixed32(out_key, family_id);
    PutFixed32(out_key, series_id);

    return Status::OK();
}

Status convert_ts_batch(DB * db, const Slice & bin, WriteBatch * write_batch) {
    DataDictionary * dict = db->GetDataDictionary();
    Slice input = bin;

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

        uint32_t family_id = dict->ToId(family);
        uint32_t series_id = dict->ToId(name);

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


}  // namespace leveldb
