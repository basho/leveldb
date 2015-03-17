// Copyright (c) 2015  Basho Technologies. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/data_dictionary.h"
#include "leveldb/env.h"
#include "db/skiplist.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace leveldb {
  
class DataDictionaryImpl {
  public:
    DataDictionaryImpl(const std::string & basedir);
    ~DataDictionaryImpl();

    bool ToId(const Slice & name, uint32_t * id) const;
    // Requires external synchronization
    void Insert(const Slice & name, uint32_t id);

    Status LoadFromFile(const std::string & filename,
                        uint64_t * last_record_offset);

    struct LookupKey {
      const Slice slice;
      LookupKey(const Slice & s) : slice(s) {}
    };

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

    Status load_status_;
    uint32_t id_;
    Arena arena_;
    port::Mutex mutex_;
    Name2IdTable name2id_;
    Id2NameTable id2name_;
    WritableFile * file_;
    log::Writer * writer_;
};

Status DataDictionary::GetLoadStatus() const {
  return impl_->load_status_;
}

DataDictionaryImpl::DataDictionaryImpl(const std::string & basedir)
  : id_(0),
  name2id_(Name2IdComparator(), &arena_),
  id2name_(Id2NameComparator(), &arena_),
  file_(NULL),
  writer_(NULL)
{
  std::string filename = basedir + "/data_dictionary.dat";
  uint64_t last_record_offset = 0;
  load_status_ = LoadFromFile(filename, &last_record_offset);
  Env * env = Env::Default();
  // TODO: If file was corrupted, move aside, start new one, assume the system
  // will try to repair it from other sources somehow.
  Status s = env->NewAppendableFile(filename, &file_, last_record_offset,
                                    4 * 1024);
  if (!s.ok())
    throw "Failed to create data dictionary output file";

  writer_ = new log::Writer(file_, last_record_offset);
}

DataDictionaryImpl::~DataDictionaryImpl() {
  if (file_) {
    file_->Close();
    delete file_;
  }
  delete writer_;
}

Status DataDictionaryImpl::LoadFromFile(const std::string & filename,
                                        uint64_t * read_offset) {
  Env * env = Env::Default();
  Status s;
  Slice input;
  uint64_t file_size;

  s = env->GetFileSize(filename, &file_size);
  if (!s.ok())
    return s;

  SequentialFile * file;
  s = env->NewSequentialFile(filename, &file);

  if (!s.ok())
    return s;

  std::string scratch;
  log::Reader reader(file, /*reporter*/ NULL, /*checksum*/ true,
                     /* init offset */ 0);
  Slice record;
  while (reader.ReadRecord(&record, &scratch)) {
    Slice name;
    uint32_t idx;
    if (!GetFixed32(&record, &idx)
        || !GetLengthPrefixedSlice(&record, &name)) {
      s = Status::Corruption("Bad record yo!!!");
      break;
    }
    Insert(name, idx);
    // assert impl_->id_ < idx
    id_ = idx;
  }
  delete file;
  return s;
}



int DataDictionaryImpl::Name2IdComparator::operator()(const char * a,
                                                      const char * b) const {
  Slice a_slice = GetLengthPrefixedSlice(a + 4);
  Slice b_slice = GetLengthPrefixedSlice(b + 4);
  return a_slice.compare(b_slice);
}

int DataDictionaryImpl::Name2IdComparator::operator()(const char * a,
                                                      const LookupKey & b)
  const {
  Slice a_slice = GetLengthPrefixedSlice(a + 4);
  Slice b_slice = b.slice;
  return a_slice.compare(b_slice);
}

int DataDictionaryImpl::Name2IdComparator::operator()(const LookupKey & a,
                                                      const char * b) const {
  Slice a_slice = a.slice;
  Slice b_slice = GetLengthPrefixedSlice(b + 4);
  return a_slice.compare(b_slice);
}

int DataDictionaryImpl::Id2NameComparator::operator() (const char * a,
                                                       const char * b) const {
  Slice a_slice(a, 4), b_slice(b, 4);
  return a_slice.compare(b_slice);
}

int DataDictionaryImpl::Id2NameComparator::operator() (uint32_t a,
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

int DataDictionaryImpl::Id2NameComparator::operator() (const char * a,
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

////////////////////////////////////////////////
// DataDictionary implementation

DataDictionary::DataDictionary(const std::string & basedir) : 
  impl_(new DataDictionaryImpl(basedir)) {}

DataDictionary::~DataDictionary() {
  delete impl_;
}

bool DataDictionary::ToString(uint32_t id, Slice * out_str) const {
  DataDictionaryImpl::Id2NameTable::Iterator it(&impl_->id2name_);
  it.Seek(id);
  if (it.Valid()
      && DataDictionaryImpl::Id2NameComparator()(id, it.key()) == 0) {
    *out_str = GetLengthPrefixedSlice(it.key() + 4);
    return true;
  } else {
    return false;
  }
}

bool DataDictionaryImpl::ToId(const Slice & name, uint32_t * id_out) const {
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

void DataDictionaryImpl::Insert(const Slice & s, uint32_t id) {
  const size_t encoded_len = 4 + VarintLength(s.size()) + s.size();
  char * buf = arena_.Allocate(encoded_len);
  EncodeFixed32(buf, id);
  char * p = buf + 4;
  p = EncodeVarint32(p, s.size());
  memcpy(p, s.data(), s.size());
  name2id_.Insert(buf);
  id2name_.Insert(buf);
  if (writer_) {
    Slice record(buf, encoded_len);
    writer_->AddRecord(record);
  }
}

uint32_t DataDictionary::ToId(const Slice & s) {
  uint32_t id;
  if (!impl_->ToId(s, &id)) {
    // Lock, try to insert, read again.
    MutexLock(&impl_->mutex_);
    if (!impl_->ToId(s, &id)) {
      id = impl_->id_++;
      impl_->Insert(s, id);
    }
  }
  return id;
}

} // leveldb namespace
