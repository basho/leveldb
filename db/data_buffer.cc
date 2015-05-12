#include "leveldb/slice.h"

namespace leveldb{

const DataBuffer &operator<<(DataBuffer &db, Slice s){
  db.clear();
  db.assign(s.data(), s.data() + s.size());
  return db;
}

}
