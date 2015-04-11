#ifndef LEVELSIZECS_H
#define LEVELSIZECS_H

#include "db/compaction_strategy.h"
#include "port/atomic_pointer.h"
#include <thread>
#include <condition_variable>
#include <chrono>

namespace leveldb{


class LevelSizeCS : public CompactionStrategy
{
public:
  LevelSizeCS();
  ~LevelSizeCS();
  virtual
  void attachTo(DBImpl *thisDB) override;
};

}
#endif // LEVELSIZECS_H
