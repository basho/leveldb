#include "level_size_cs.h"
#include "db/db_impl.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "util/hot_threads.h"
#include "util/thread_tasks.h"

namespace leveldb{



LevelSizeCS::LevelSizeCS()
{
}

LevelSizeCS::~LevelSizeCS()
{
}

void LevelSizeCS::attachTo(DBImpl *db)
{
  db->addWriteListener([db]{
    for (int l = 0; l < config::kNumLevels; l++){
      auto numFiles = db->versions_->NumLevelFiles(l);
      if ( numFiles > db->compactionTarget_.numFilesPerLevel(l) )
        db->enqueueCompaction(l);
    }
    for (int l = 0; l < config::kNumLevels; l++){
      auto flist = db->versions_->current()->GetFileList(l);
      auto tableCache = db->versions_->GetTableCache();
      uint64_t acc = 0;
      for ( auto f: flist)
        acc += tableCache->GetStatisticValue(f->number, eSstCountDeleteKey);
      if (db->options_.delete_threshold < acc )
        db->enqueueCompaction(l);
    }
  });
  db->addCompactionFinishedListener([db](int level){
    db->DeleteObsoleteFiles();
    if ( level+1 >= config::kNumLevels )
      return;
    if ( db->versions_->NumLevelFiles(level+1) > db->compactionTarget_.numFilesPerLevel(level+1) )
      db->enqueueCompaction(level+1);
    auto numFiles = db->versions_->NumLevelFiles(0);
    if ( numFiles > db->compactionTarget_.numFilesPerLevel(0) )
      db->enqueueCompaction(0);
    for (int l = 0; l < config::kNumLevels; l++){
      if ( db->numRangeDeleteIntervals(l) > db->compactionTarget_.numRangeDeletes() )
        db->enqueueCompaction(l);
    }
  });
}



}
