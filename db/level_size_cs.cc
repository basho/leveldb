#include "level_size_cs.h"
#include "db/db_impl.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "util/hot_threads.h"
#include "util/thread_tasks.h"

namespace leveldb{

class CompactionCheckTask : public ThreadTask
{
protected:
    DBImpl *db;
    port::AtomicPointer  *stopped;

public:
    CompactionCheckTask(DBImpl * Db, port::AtomicPointer  *stoped)
        : db(Db), stopped(stoped)  {};

    virtual void operator()()
    {
      if ( db->shutting_down_.Acquire_Load() ){
        stopped->Release_Store(this);
        db->bg_cv_.SignalAll();
        return;
      }
      db->env_->SleepForMicroseconds(1000000);
      MutexLock l(&db->mutex_);
      bool gotSomethingToDo = false;
      for (int l = 0; l < config::kNumLevels; l++){
        auto numFiles = db->versions_->NumLevelFiles(l);
        if ( numFiles > db->compactionTarget_.numFilesPerLevel(l) ){
          db->enqueueCompaction(l);
          gotSomethingToDo = true;
        }
      }
      if (!gotSomethingToDo){
        // while we seemingly aren't under a heavy load, perhaps it's time to consider decrising amounts of tombstones
        for (int l = 0; l < config::kNumLevels; l++){
          auto flist = db->versions_->current()->GetFileList(l);
          auto tableCache = db->versions_->GetTableCache();
          uint64_t acc = 0;
          for ( auto f: flist)
            acc += tableCache->GetStatisticValue(f->number, eSstCountDeleteKey);
          if (db->options_.delete_threshold <= acc )
          {
            db->enqueueCompaction(l);
            gotSomethingToDo = true;
          }
        }
      }
      gLevel0Threads->Submit(new CompactionCheckTask(db, stopped));
    };

    CompactionCheckTask(const CompactionCheckTask &) = delete;
    CompactionCheckTask & operator=(const CompactionCheckTask &) = delete;

};  // class CompactionTask


LevelSizeCS::LevelSizeCS() : stopped(this)
{
}

void LevelSizeCS::attachTo(DBImpl *db)
{
  gLevel0Threads->Submit(new CompactionCheckTask(db, &stopped));
  stopped.Release_Store(nullptr);
  db->addWriteListener([db]{
    if ( db->versions_->NumLevelFiles(0) )
      db->enqueueCompaction(0);
  });
  db->addCompactionFinishedListener([db](int level){
    db->DeleteObsoleteFiles();
    if ( db->versions_->NumLevelFiles(level+1) > db->compactionTarget_.numFilesPerLevel(level+1) )
      db->enqueueCompaction(level+1);
  });
}

bool LevelSizeCS::isBackgroundJobs() const
{
  return !stopped.Acquire_Load();

}


}
