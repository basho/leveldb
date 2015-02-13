// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include <string>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

// Time series key comparator.
// Assumes first 8 bytes are a little endian 64 bit unsigned integer,
// followed by family name size encoded in 2 little endian bytes,
// followed by the bytes forming the series name.
// TODO: Implement data dictionary, which changes the second part of the key
// from a byte array to a 4 byte little endian 32 bit unsigned integer.
// TODO: Add series family. A variable length string before the series name.
// When the family is present, sorting changes to (family, time, series).
// Without a family, it's (series, time).
class TSComparator : public Comparator {
  protected:

  public:
    TSComparator() { }

    virtual const char* Name() const {
      return "TSComparator";
    }

    static Slice GetTimeSlice(const Slice & s) {
      return Slice(s.data(), 8);
    }

    static Slice GetFamilySlice(const Slice & s) {
      return Slice(s.data() + 8, 4);
    }

    static Slice GetSeriesSlice(const Slice & s) {
      return Slice(s.data() + 12, 4);
    }

    static bool HasFamily(const Slice & s) {
      return s[8] || s[9] || s[10] || s[11];
    }

    virtual int Compare(const Slice& a, const Slice& b) const {
      Slice afam = GetFamilySlice(a), bfam = GetFamilySlice(b);
      int fam_cmp = afam.compare(bfam);

      if (fam_cmp)
        return fam_cmp;

      // Here, either same family or neither in a family.
      // If same family, sort by (time, series).
      if (HasFamily(a)) {
        Slice a_time = GetTimeSlice(a), b_time = GetTimeSlice(b);
        int time_cmp = a_time.compare(b_time);

        if (time_cmp)
          return time_cmp;

        Slice a_series = GetSeriesSlice(b), b_series = GetSeriesSlice(b);
        return a_series.compare(b_series);
      } else {
        // No family, so sort by (series, time)
        Slice a_series = GetSeriesSlice(b), b_series = GetSeriesSlice(b);
        int series_cmp = a_series.compare(b_series);

        if (series_cmp)
          return series_cmp;

        Slice a_time = GetTimeSlice(a), b_time = GetTimeSlice(b);
        return a_time.compare(b_time);
      }
    }

    // No need to shorten keys since it's fixed size.
    virtual void FindShortestSeparator(std::string* start,
                                       const Slice& limit) const {
    }

    // No need to shorten keys since it's fixed size.
    virtual void FindShortSuccessor(std::string* key) const {
    }

};

}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise = NULL;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

static port::OnceType ts_once = LEVELDB_ONCE_INIT;
static const Comparator* ts_cmp = NULL;

static void InitTSComparator() {
  ts_cmp = new TSComparator();
}

const Comparator* GetTSComparator() {
  port::InitOnce(&ts_once, InitTSComparator);
  return ts_cmp;
}
void ComparatorShutdown()
{
    delete bytewise;
    bytewise = NULL;
    delete ts_cmp;
    ts_cmp = NULL;
}

}  // namespace leveldb
