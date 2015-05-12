#ifndef COMPARABLEKEY_H
#define COMPARABLEKEY_H

#include "db/dbformat.h"
#include "db/data_buffer.h"

namespace leveldb{


class ComparableKey
{
public:
  class RefTag{};
  ComparableKey();
  ComparableKey(const Comparator *cmp, Slice key );
  ComparableKey(const Comparator *cmp, Slice key, RefTag );
  bool operator< (const ComparableKey& other) const;
  //bool operator< (Slice otherKey) const;
private:
  const Comparator  *cmp_;
  DataBuffer  key_;
  Slice       s_;
};



}

#endif // COMPARABLEKEY_H
