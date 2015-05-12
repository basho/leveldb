#ifndef RANGEDELETES_H
#define RANGEDELETES_H

#include <boost/icl/interval_map.hpp>
#include <memory>
#include <algorithm>
#include <shared_mutex>
#include <vector>
#include <array>
#include <fstream>
#include "db/comparable_key.h"
#include "leveldb/slice.h"
#include "db/version_set.h"

namespace leveldb{

/**
 * Does not require external synchronization.
 */
class RangeDeletes
{
  typedef boost::icl::interval_map
  <leveldb::ComparableKey, SequenceNumber, boost::icl::partial_absorber, std::less, boost::icl::inplace_max> IntervalMap;
public:
  RangeDeletes( const std::string &dbPath, Env *env, const Comparator *userKeyCmp );

  class LevelSnapshot{
  public:
    LevelSnapshot() : owner_(nullptr){};
    ~LevelSnapshot();
    /// just to circumvent GCC inability to move lambdas
    //LevelSnapshot(LevelSnapshot &s) noexcept : LevelSnapshot(std::move(s)){};
    LevelSnapshot(LevelSnapshot &&s) noexcept;
    LevelSnapshot &operator= (LevelSnapshot &&s) noexcept;
    bool isDeleted(Slice key, SequenceNumber seq) const;
  private:
    LevelSnapshot(RangeDeletes *owner, int level, const IntervalMap *im) :
      owner_(owner), level_(level), intervals_(im){}
    RangeDeletes *owner_;
    int level_;
    const IntervalMap *intervals_;
    friend class RangeDeletes;
  };

  class DBSnapshot{
  public:
    bool isDeleted(Slice key, SequenceNumber seq) const;
  private:
    std::vector<std::shared_ptr<IntervalMap>> intervals_;
    RangeDeletes *owner_;
    friend class RangeDeletes;
  };
  /// Only one snapshot per level can exist at a time.
  LevelSnapshot createSnapshot(int level);
  DBSnapshot dbSnapshot();
  /// this is going to move the snapshot to the level from the level it was taken
  /// level \a toLevel shall not have alive level snapshot created from it
  /// if the \a toLevel is the last one, call drop() instead
  void append(int toLevel, LevelSnapshot &&s);
  void drop(LevelSnapshot &&s);
  void addInterval(Slice from, Slice to, SequenceNumber seq);
  bool isDeleted(Slice key, SequenceNumber seq);
  size_t numRanges(int level);
private:
  const std::string dbPath_;
  Env *env_;
  const Comparator *cmp_;

  typedef uint64_t FileNum;

  struct Level{
    /// has to check all intervals but change only the intervals[1]
    std::array<std::shared_ptr<IntervalMap>, 2> intervals;
    std::vector<FileNum> logs;
  };
  std::vector<Level> levels_;
  std::shared_timed_mutex mutex_; // protects changes to all of above
  FileNum         currentLogFile_ = 0;
  std::ofstream   log_;
  std::mutex      logMutex_; //protects the two things above

  void releaseSnapshot(int level);
  void ensureCapacity(int level);
  std::string logFilename(int level, FileNum num);
  /// does nothing if is not open
  /// \a mutex_ and \a logMutex_ have to be released before the call
  void closeLog();
  /// does nothing if already open
  /// shuld be called under logMutex_
  void openLog();
  /// return true, to stop visiting
  void visitAllIntervals(const std::function<bool(std::shared_ptr<RangeDeletes::IntervalMap> &im)> &);

  static const uint16_t logFileVersion = 0;
};


}
#endif // RANGEDELETES_H
