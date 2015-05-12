#include "range_deletes.h"
#include <cstdio>
#include "util/format.h"

namespace leveldb{

using namespace std;

RangeDeletes::RangeDeletes(const std::string &dbPath, Env *env, const Comparator *userKeyCmp)
  : dbPath_(dbPath), env_(env), cmp_(userKeyCmp)
{
  log_.exceptions(ofstream::failbit | ofstream::badbit);
}

RangeDeletes::LevelSnapshot RangeDeletes::createSnapshot(int level)
{
  {
    unique_lock<std::shared_timed_mutex> l(mutex_);
    ensureCapacity(level);
    auto &lvl = levels_[level];
    assert( !lvl.intervals[0] ); // only 1 snapshot per level allowed
    assert( lvl.intervals[1] );
    lvl.intervals[0] = move(lvl.intervals[1]);
    lvl.intervals[1] = make_shared<IntervalMap>();
  }
  if ( level == 0 )
    closeLog();
  {
    shared_lock<std::shared_timed_mutex> l(mutex_);
    auto &lvl = levels_[level];
    return LevelSnapshot(this, level, lvl.intervals[0].get());
  }
}

RangeDeletes::DBSnapshot RangeDeletes::dbSnapshot()
{
  DBSnapshot ret;
  visitAllIntervals([&](shared_ptr<IntervalMap> &im)->bool{
    ret.intervals_.push_back(im);
    return false;
  });
  ret.owner_ = this;
  return ret;
}

void RangeDeletes::append(int toLevel, RangeDeletes::LevelSnapshot &&s)
{
  assert(toLevel);
  assert(s.owner_);
  s.owner_ = nullptr;
  unique_lock<std::shared_timed_mutex> l(mutex_);
  ensureCapacity(toLevel);
  auto &toLvl = levels_[toLevel];
  auto &fromLvl = levels_[s.level_];
  assert( !toLvl.intervals[0] ); // level, applied to, shall not have level snapshot created from it
  *toLvl.intervals[1] += *fromLvl.intervals[0];
  fromLvl.intervals[0].reset();
  auto vc = fromLvl.logs;
  fromLvl.logs.clear();
  toLvl.logs.insert( toLvl.logs.end(), begin(vc), end(vc) );
  l.unlock();
  for ( FileNum f: vc ){
    string fromFile = logFilename(s.level_, f);
    string toFile = logFilename(toLevel, f);
    if (std::rename(fromFile.c_str(), toFile.c_str()))
      throw runtime_error(Format("Can't rename \"%1%\" to \"%2%\": %3%") % fromFile % toFile % strerror(errno) );
  }
}

void RangeDeletes::drop(RangeDeletes::LevelSnapshot &&s)
{
  assert(s.owner_);
  s.owner_ = nullptr;
  unique_lock<std::shared_timed_mutex> l(mutex_);
  auto &fromLvl = levels_[s.level_];
  fromLvl.intervals[0].reset();
  auto vc = fromLvl.logs;
  fromLvl.logs.clear();
  l.unlock();
  for ( FileNum f: vc ){
    string fromFile = logFilename(s.level_, f);
    if (std::remove(fromFile.c_str()))
      throw runtime_error(Format("Can't remove \"%1%\": %2%") % fromFile % strerror(errno) );
  }
}

void RangeDeletes::releaseSnapshot(int level)
{
  unique_lock<std::shared_timed_mutex> l(mutex_);
  auto &lvl = levels_[level];
  *lvl.intervals[1] += *lvl.intervals[0];
  lvl.intervals[0].reset();
}

void RangeDeletes::ensureCapacity(int level)
{
  if ( levels_.size() > level )
    return;
  size_t dif = level + 1 - levels_.size();
  levels_.resize(level+1);
  while ( dif-- ){
    levels_[level--].intervals[1] = make_shared<IntervalMap>();
  }
}

void RangeDeletes::closeLog()
{
  FileNum logNum;
  {
    unique_lock<std::mutex> l(logMutex_);
    if ( !log_.is_open() )
      return;
    log_.close();
    logNum = currentLogFile_;
  }
  {
    unique_lock<std::shared_timed_mutex> l(mutex_);
    levels_[0].logs.push_back(logNum);
  }
}

void RangeDeletes::openLog()
{
  if ( log_.is_open() )
    return;
  currentLogFile_++;
  log_.open(logFilename(0, currentLogFile_), ios_base::binary);
  log_ << logFileVersion;
}

void RangeDeletes::visitAllIntervals(const std::function<bool (shared_ptr<RangeDeletes::IntervalMap> &)> &f)
{
  shared_lock<std::shared_timed_mutex> l(mutex_);
  for (auto &lvl : levels_){
    for (auto &im : lvl.intervals){
      if ( im && f(im) )
        break;
    }
  }
}

string RangeDeletes::logFilename(int level, RangeDeletes::FileNum num)
{
  ostringstream logPath;
  assert( dbPath_.back() != '/' );
  logPath << dbPath_  << "/sst_" << level << "/rd" << num;
  return logPath.str();
}

void RangeDeletes::addInterval(Slice from, Slice to, SequenceNumber seq)
{
  {
    ComparableKey f(cmp_, from);
    ComparableKey t(cmp_, to);
    unique_lock<std::shared_timed_mutex> l(mutex_);
    ensureCapacity(0);
    levels_[0].intervals[1]->add( make_pair(IntervalMap::interval_type::open(f, t), seq) );
  }
  unique_lock<std::mutex> l(logMutex_);
  openLog();
  log_ << from << to << seq << flush;
}

bool RangeDeletes::isDeleted(Slice key, SequenceNumber seq)
{
  ComparableKey k(cmp_, key, ComparableKey::RefTag());
  bool deleted = false;
  visitAllIntervals([&](auto &im)->bool{
    auto i = im->find(k);
    if ( i != im->end() && i->second >= seq ){
      deleted = true;
      return true;
    }
    return false;
  });
  return deleted;
}

size_t RangeDeletes::numRanges(int level)
{
  shared_lock<std::shared_timed_mutex> l(mutex_);
  if ( level >= levels_.size() )
    return 0;
  auto &ivls = levels_[level].intervals;
  size_t acc = 0;
  for ( auto &im : ivls ){
    if (im)
      acc += interval_count(*im);
  }
  return acc;
}

RangeDeletes::LevelSnapshot::~LevelSnapshot()
{
  if (owner_)
    owner_->releaseSnapshot(level_);
}

RangeDeletes::LevelSnapshot::LevelSnapshot(RangeDeletes::LevelSnapshot &&s) noexcept
{
  owner_ = s.owner_;
  level_ = s.level_;
  intervals_ = s.intervals_;
  s.owner_ = nullptr;
}

RangeDeletes::LevelSnapshot &RangeDeletes::LevelSnapshot::operator=(RangeDeletes::LevelSnapshot &&s) noexcept
{
  assert(owner_ == nullptr);
  owner_ = s.owner_;
  level_ = s.level_;
  intervals_ = s.intervals_;
  s.owner_ = nullptr;
  return *this;
}

bool RangeDeletes::LevelSnapshot::isDeleted(Slice key, SequenceNumber seq) const
{
  ComparableKey k(owner_->cmp_, key, ComparableKey::RefTag());
  auto i = intervals_->find(k);
  if ( i != intervals_->end() && i->second >= seq )
    return true;
  return false;
}

bool RangeDeletes::DBSnapshot::isDeleted(Slice key, SequenceNumber seq) const
{
  ComparableKey k(owner_->cmp_, key, ComparableKey::RefTag());
  shared_lock<std::shared_timed_mutex> l(owner_->mutex_);
  for ( auto &im : intervals_ ){
    auto i = im->find(k);
    if ( i != im->end() && i->second >= seq )
      return true;
  }
  return false;
}

}
