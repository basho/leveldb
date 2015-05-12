#ifndef DATABUFFER_H
#define DATABUFFER_H

#include <vector>
#include <limits>
#include <cstdint>
#include <istream>
#include <ostream>

namespace leveldb{

typedef std::vector<uint8_t> DataBuffer;

// corresponding write is in slice.h
template <class CharT, class Traits>
std::basic_istream<CharT, Traits>& operator>>(std::basic_istream<CharT, Traits>& is, DataBuffer &b){
  std::istream::sentry s(is);
  if (!s)
    return is;
  DataBuffer::size_type size = 0;
  int shift = 0;
  for ( DataBuffer::size_type v = is.get(); ; v = is.get()) {
    if ( shift >= std::numeric_limits<DataBuffer::size_type>::digits )
      is.setstate(std::ios_base::failbit);
    size |= (v & 127) << shift;
    if ( !(v & 128) )
      break;
    shift += 7;
  }
  auto oldSize = b.size();
  b.resize(b.size() + size);
  is.read(&b[oldSize], size);
  return is;
}


}

#endif // DATABUFFER_H
