#include "comparable_key.h"


namespace leveldb{

ComparableKey::ComparableKey()
{
  cmp_ = nullptr;
}

ComparableKey::ComparableKey(const Comparator *cmp, Slice key) : cmp_(cmp), key_(key.data(), key.data() + key.size())
{

}

ComparableKey::ComparableKey(const Comparator *cmp, Slice key, ComparableKey::RefTag) : cmp_(cmp), s_(key)
{

}

bool ComparableKey::operator<(const ComparableKey &other) const
{
  assert(cmp_ == other.cmp_); // you'd probably like them to coincide
  Slice ok = other.s_.empty() ? other.key_ : other.s_;
  Slice tk = s_.empty() ? key_ : s_;
  return cmp_->Compare(tk, ok) < 0;
}

//bool ComparableKey::operator<(Slice other) const
//{
//  assert( cmp_ );
//  return cmp_->Compare(key_, other) < 0;
//}

}

