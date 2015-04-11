#pragma once

namespace leveldb{

class DBImpl;

class CompactionStrategy
{
public:
  CompactionStrategy(){};
  virtual
  ~CompactionStrategy(){};

  /**
   * attaches itself to a database. possibly subscribing to interesting evenets
   * @param thisDB
   */
  virtual
  void attachTo(DBImpl *thisDB) =0;
};

}

