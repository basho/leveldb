#ifndef LEVELSIZECS_H
#define LEVELSIZECS_H

#include "db/compaction_strategy.h"
#include "port/atomic_pointer.h"

namespace leveldb{


class LevelSizeCS : public CompactionStrategy
{
public:
  LevelSizeCS();
  virtual
  void attachTo(DBImpl *thisDB) override;
  virtual
  bool isBackgroundJobs() const override;
private:
  port::AtomicPointer stopped;
};

}
#endif // LEVELSIZECS_H
