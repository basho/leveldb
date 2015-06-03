#include "util/classes/ExceptionUtils.h"
#include "util/classes/StatManager.h"

#include <cmath>
#include <iomanip>
#include <signal.h>
#include <sys/time.h>
#include <time.h>

using namespace std;
using namespace leveldb::util;

#define TRYLOCK(fn)\
  if(noOp_)\
    return;\
  try {\
    lock();\
    fn;\
  } catch(...) {\
  };\
  unlock();

/**.......................................................................
 * Constructors
 */
StatManager::StatManager()
{
  initialize(60, 1, false);
}

StatManager::StatManager(unsigned strobeIntervalSec, unsigned queueLen, bool noOp) 
{
  initialize(strobeIntervalSec, queueLen, noOp);
}

/**.......................................................................
 * Common initialization
 */
void StatManager::initialize(unsigned strobeIntervalSec, unsigned queueLen, bool noOp) 
{
  strobeIntervalSec_ = strobeIntervalSec;
  queueLen_          = queueLen;
  strobeThreadId_    = 0;
  noOp_              = noOp;
}

/**.......................................................................
 * Destructor.
 */
StatManager::~StatManager() 
{
  if(strobeThreadId_ != 0) {
    pthread_cancel(strobeThreadId_);
    strobeThreadId_ = 0;
  }
}

/**.......................................................................
 * Mutex locking/unlocking
 */
void StatManager::lock() 
{
  mutex_.Lock();
}

void StatManager::unlock() 
{
  mutex_.Unlock();
}

/**.......................................................................
 * Spawn the thread that will strobe counters managed by this object,
 * storing the differential count on each strobe into a circular
 * buffer of length queueLen_
 */
void StatManager::spawnStrobeThread()
{
  pthread_create(&strobeThreadId_, 0, StatManager::startStrobeThread, this);
}

/**.......................................................................
 * Static entry function for the strobe thread
 */
THREAD_START(StatManager::startStrobeThread)
{
  StatManager* statManager = (StatManager*)arg;
  return statManager->runStrobeThread(arg);
}

/**.......................................................................
 * Run method for the strobe thread
 */
void* StatManager::runStrobeThread(void* arg)
{
  StatManager* sm = (StatManager*) arg;
  struct timeval timeout, currentTime;

  while(true) {

    //------------------------------------------------------------
    // Calculate the absolute second boundary on which we want to fire
    // next
    //------------------------------------------------------------

    gettimeofday(&currentTime, 0);

    double currentSec   = currentTime.tv_sec + (double)(currentTime.tv_usec) / 1e6;

    uint64_t nearestSec = llrint(currentSec);
    double nextSec      = nearestSec + sm->strobeIntervalSec_;
    double timeoutSec   = nextSec - currentSec;
    
    timeout.tv_sec  = (time_t)(timeoutSec);
    timeout.tv_usec = (time_t)((timeoutSec - timeout.tv_sec) * 1e6);

    select(0, NULL, NULL, NULL, &timeout);

    gettimeofday(&currentTime, 0);
    currentSec   = currentTime.tv_sec + (double)(currentTime.tv_usec) / 1e6;

    //------------------------------------------------------------
    // For convenience, strobe the counters with the closest absolute
    // second boundary as the timestamp
    //------------------------------------------------------------

    sm->strobe(llrint(currentSec));
  }
}

/**.......................................................................
 * Strobe all counters managed by this object
 */
void StatManager::strobe(uint64_t time)
{
  TRYLOCK(
    for(std::map<std::string, Counter>::iterator iter=counterMap_.begin(); iter != counterMap_.end();
	iter++) 
      iter->second.storeDifferential(time);
	  );
}

void StatManager::initCounter(std::string name)
{
  std::map<std::string, uint64_t> addMap;
  addMap[name] = 0;

  add(addMap);
}

/**.......................................................................
 * Add to a set of counters
 */
void StatManager::add(std::map<std::string, uint64_t>& addMap)
{
  TRYLOCK(

     for(std::map<std::string, uint64_t>::iterator iter=addMap.begin(); 
	iter != addMap.end(); iter++) {

      // If the counter doesn't already exist, add an entry and resize the buffer

      if(counterMap_.find(iter->first)==counterMap_.end()) {

	Counter& counter = counterMap_[iter->first];
	counter.resize(queueLen_);
	counter.add(iter->second);

	// Else just add to the existing counter

      } else {
	Counter& counter = counterMap_[iter->first];
	counter.add(iter->second);
      }
    }
	  );
}

/**.......................................................................
 * Iterate through the map of requested counters, filling in data
 * where available
 */
void StatManager::getCounts(std::map<std::string, Sample>& sampleMap,
			    unsigned nSample) 
{
  TRYLOCK(

    for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); iter++) {

      // Is this counter in our map? If so, retrieve the data

      if(counterMap_.find(iter->first) != counterMap_.end()) {

	Counter& counter = counterMap_[iter->first];
	counter.getCounts(iter->second, nSample);

	// Else ensure that the returned array is zero-sized and
	// marked as not found

      } else {
	iter->second.found_ = false;
	iter->second.total_ = 0;
	iter->second.differentials_.resize(0);
      }
    }
	  );
}

/**.......................................................................
 * Methods to control output formatting
 */
void StatManager::setOutputOrder(std::vector<std::string>& order)
{
  outputOrder_ = order;
}

void StatManager::setOutputDivisors(std::map<std::string, double>& divisorMap)
{
  outputDivisors_ = divisorMap;
}

double StatManager::getDivisor(std::string name)
{
  if(outputDivisors_.find(name) != outputDivisors_.end()) {
    return outputDivisors_[name];
  } else {
    return 1.0;
  }
}

void StatManager::setOutputLabels(std::map<std::string, std::string>& labelMap)
{
  outputLabels_ = labelMap;
}

std::string StatManager::getLabel(std::string name)
{
  if(outputLabels_.find(name) != outputLabels_.end()) {
    return outputLabels_[name];
  } else {
    return name;
  }
}

void StatManager::setOutputUnits(std::map<std::string, std::string>& unitMap)
{
  outputUnits_ = unitMap;
}

std::string StatManager::getUnits(std::string name)
{
  if(outputUnits_.find(name) != outputUnits_.end()) {
    return outputUnits_[name];
  } else {
    return "?";
  }
}

std::string StatManager::getHeader(std::string name)
{
  ostringstream os;
  os << getLabel(name) << "(" << getUnits(name) << ")";
  return os.str();
}

/**.......................................................................
 * Return the longest header we will encounter
 */
unsigned StatManager::longestHeader(std::map<std::string, Sample>& sampleMap)
{
  bool first=true;
  size_t maxLen=0;

  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    Sample& sample = iter->second;

    if(first) {
      first = false;
      maxLen = getHeader(iter->first).size();
    } else {
      unsigned len = getHeader(iter->first).size();
      maxLen = (maxLen > len ? maxLen : len);
    }

    if(sample.differentials_.size() > 0) {
      uint64_t time = sample.differentials_[sample.differentials_.size()-1].first;
      size_t timeLen = formattedDate(time).size();
      maxLen = (maxLen > timeLen ? maxLen : timeLen);
    }
  }

  return maxLen;
}

/**.......................................................................
 * Return the name of the counter with the longest sample array
 */
std::string StatManager::longestCounter(std::map<std::string, Sample>& sampleMap)
{
  bool first=true;
  size_t maxLen=0;
  std::string maxCounter;

  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    if(first) {
      first = false;
      maxCounter = iter->first;
      maxLen = iter->second.differentials_.size();
    } else {
      std::string counter = iter->first;
      unsigned len = iter->second.differentials_.size();

      if(len > maxLen) {
	maxLen     = len;
	maxCounter = counter;
      }
    }
  }

  return maxCounter;
}

/**.......................................................................
 * Format output for counters managed by this class
 */
std::string StatManager::formatOutput(std::string header, 
				      std::map<std::string, Sample>& sampleMap)
{
  unsigned nCounter = sampleMap.size();
  size_t headerLen = longestHeader(sampleMap) + 2;
  size_t lineLen = (nCounter+1) * headerLen;

  std::ostringstream os;

  for(unsigned i=0; i < lineLen; i++)
    os << "=";
  os << std::endl;

  os << std::setw(lineLen) << std::left << header << std::endl;

  for(unsigned i=0; i < lineLen; i++)
    os << "-";
  os << std::endl;

  os << std::setw(headerLen) << " ";

  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    os << std::setw(headerLen) << std::left << getHeader(iter->first);
  }

  os << std::endl;
    
  os << "Totals";
  for(unsigned i=0; i < lineLen-strlen("Totals"); i++)
    os << "-";
  os << std::endl << std::endl;

  os << std::setw(headerLen) << " ";
  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    os << std::setw(headerLen) << std::left << std::setprecision(5) << iter->second.total_ / getDivisor(iter->first);
  }
  os << std::endl << std::endl;

  os << "Differentials";
  for(unsigned i=0; i < lineLen-strlen("Differentials"); i++)
    os << "-";
  os << std::endl << std::endl;

  std::string maxCounterName = longestCounter(sampleMap);
  Sample& maxSample = sampleMap[maxCounterName];

  for(unsigned i=0; i < maxSample.differentials_.size(); i++) {
      
    os << std::setw(headerLen) << std::left << formattedDate(maxSample.differentials_[i].first);

    for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
	iter++) {
      Sample& samples = iter->second;

      if(i+1 > samples.differentials_.size()) {
	os << std::setw(headerLen) << std::left << "--";
      } else {
	os << std::setw(headerLen) << std::left << samples.differentials_[i].second / getDivisor(iter->first);
      }
    }
    os << std::endl;
  }

  for(unsigned i=0; i < lineLen; i++)
    os << "=";
  os << std::endl;

  return os.str();
}

//=======================================================================
// Methods of StatManager::Counter
//=======================================================================

StatManager::Counter::Counter()
{
  integratedCurr_ = 0;
  integratedLast_ = 0;
  first_ = true;
  resize(0);
}
/**.......................................................................
 * Resize the data buffers for this counter
 */
void StatManager::Counter::resize(unsigned queueLen)
{
  differentials_.resize(queueLen);
  sampleTimes_.resize(queueLen);
}

/**.......................................................................
 * Add to the counter
 */
void StatManager::Counter::add(uint64_t val)
{
  integratedCurr_ += val;
}

/**.......................................................................
 * Store the current differential count
 */
void StatManager::Counter::storeDifferential(uint64_t time)
{
  uint64_t diff;
  
  // By definition, we can't store differentials until the second
  // strobe

  if(first_) {
    first_ = false;

  } else {

    diff = integratedCurr_ - integratedLast_;
    differentials_.push(diff);
    sampleTimes_.push(time);
  }

  integratedLast_ = integratedCurr_;
}

/**.......................................................................
 * Retrieve the last nSample differential counts for this counter
 * (most recent will be the first element in the returned arrays)
 */
void StatManager::Counter::getCounts(Sample& sample,
				     unsigned nSample)
{
  sample.total_ = integratedCurr_;

  std::valarray<uint64_t> times = sampleTimes_.reverseCopy();
  std::valarray<uint64_t> vals  = differentials_.reverseCopy();

  unsigned len = vals.size();
  len = ((nSample > 0 && nSample < len) ? nSample : len);

  sample.differentials_.resize(len);

  for(unsigned i=0; i < len; i++) {
    sample.differentials_[i].first  = times[i];
    sample.differentials_[i].second = vals[i];
  }
}

std::string StatManager::formattedDate(uint64_t sec)
{
  time_t time = sec;

  struct tm* date = localtime(&time);
  char time_string[40];
  strftime (time_string, sizeof (time_string), "%Y-%m-%d %H:%M:%S", date);

  return time_string;
}
