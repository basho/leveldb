// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <syslog.h>
#include <algorithm>
#include <stdint.h>
#include <string>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/coding.h"

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

// Bigsets key comparator (copied and pasted from Engel's
// https://github.com/basho/leveldb/blob/prototype/timeseries/util/comparator.cc#L69
//
// @TODO add delegation to default comparator. Add a first byte(s?) that identifies bigsets
// Clock keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $c, %% means clock
//    Actor/binary, %% The actual actor
//    >>
// Handoff Filter keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $d, %% means handoff filter
//    Actor/binary, %% The actual actor
//    >>
// Element keys are
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the length of the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $e, % indicates an element
//    ElementLen:32/little-unsigned-integer, %% Length of the element
//    Element:ElementLen/binary, %% The actual element
//    ActorLen:32/little-unsigned-integer, %% Length of the actor ID
//    Actor:ActorLen/binary, %% The actual actor
//    Counter:64/little-unsigned-integer,
//    $a | $r:1/binary, %% a|r single byte char to determine if the key is an add or a tombstone
//    >>
//  End Key is:
//  <<
//    SetNameLen:32/little-unsigned-integer, %% the lengthof the set name
//    SetName:SetNameLen/binary, %% Set name for bytewise comparison
//    $z, %% means end key, used for limiting streaming fold
//    >>

class BSComparator : public Comparator {
  protected:

  public:
    BSComparator() { }

    virtual const char* Name() const {
      return "BSComparator";
    }

  static Slice Get32PrefData( Slice &s) {
    uint32_t actual = DecodeFixed32(s.data());
    Slice res =  Slice(s.data() +4, actual);
    s.remove_prefix(4+actual);
    return res;
  }

  static uint64_t GetCounter( Slice &s) {
    uint64_t actual = DecodeFixed64(s.data());
    s.remove_prefix(8);
    return actual;
  }

  static Slice GetTsb( Slice &s) {
    Slice res = Slice(s.data(), 1); // one byte a or r
    s.remove_prefix(1);
    return res;
  }

  static Slice GetKeyType(Slice &s) {
    Slice res = Slice(s.data(), 1); // one byte c, d, e, or z
    s.remove_prefix(1);
    return res;
  }

  static bool IsClock(Slice &s) {
    return s[0] == 'c';
  }

  static bool IsHoff(Slice &s) {
    return s[0] == 'd';
  }

    virtual int Compare(const Slice& a, const Slice& b) const {
      if(a == b) {
        return 0;
      }

      Slice ac = Slice(a.data()), bc = Slice(b.data());
      Slice aset = Get32PrefData(ac), bset = Get32PrefData(bc);

      int set_cmp = aset.compare(bset);

      if(set_cmp) {
        return set_cmp;
      }
      // Same set?
      // check keytype byte (c=clock, d=hoff, e=element, z=end_key)
      Slice a_keytype = GetKeyType(ac), b_keytype = GetKeyType(bc);

      int key_type_cmp = a_keytype.compare(b_keytype);

      if(key_type_cmp) {
        return key_type_cmp;
      }

      // same type & same set, but not the same key? can't be an end key!
      if(IsClock(a_keytype) || IsHoff(a_keytype)) {
        // compare actor, actor is only data left in key, so just
        // compare slices
        return ac.compare(bc);
      }

      // compare element etc
      Slice aelem = Get32PrefData(ac), belem = Get32PrefData(bc);
      int elem_cmp = aelem.compare(belem);

      if(elem_cmp) {
        return elem_cmp;
      }

      //if here, same element (even if element is absent (a clock))
      // so check the actor
      //if here, same element (even if element is absent (a clock))
      // so check the actor
      Slice a_actor = Get32PrefData(ac), b_actor = Get32PrefData(bc);

      int actor_cmp = a_actor.compare(b_actor);

      if(actor_cmp) {
        return actor_cmp;
      }

      // If here, then same actor, which means same key (for a clock)
      // or check counter (for an element). Since same key is dealt
      // with by == above, must be an element key

      uint64_t a_cntr = GetCounter(ac), b_cntr = GetCounter(bc);

      uint64_t cntr_cmp = a_cntr - b_cntr;
      if(cntr_cmp) {
        return cntr_cmp;
      }

      // same set, element, actor and counter but not the same key?
      // Look at the TSB
      Slice a_tsb = GetTsb(ac), b_tsb = GetTsb(bc);

      return  a_tsb.compare(b_tsb);
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

static port::OnceType bs_once = LEVELDB_ONCE_INIT;
static const Comparator* bs_cmp = NULL;

static void InitBSComparator() {
  bs_cmp = new BSComparator();
}

const Comparator* GetBSComparator() {
  port::InitOnce(&bs_once, InitBSComparator);
  return bs_cmp;
}
void ComparatorShutdown()
{
    delete bytewise;
    bytewise = NULL;
    delete bs_cmp;
    bs_cmp = NULL;
}

}  // namespace leveldb
