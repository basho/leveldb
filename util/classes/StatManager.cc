#include "util/classes/ExceptionUtils.h"
#include "util/classes/StatManager.h"

#include <cmath>
#include <iomanip>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <time.h>

using namespace std;
using namespace leveldb::util;

#define TRYLOCK(fn)				\
  if(noOp_)					\
    return;					\
  try {						\
    lock();					\
    fn;						\
  } catch(...) {				\
  };						\
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

StatManager::StatManager(const StatManager& sm)
{
  *this = sm;
}

StatManager::StatManager(StatManager& sm)
{
  *this = sm;
}

void StatManager::operator=(const StatManager& sm)
{
  *this = (StatManager&)sm;
}

void StatManager::operator=(StatManager& sm)
{
  strobeIntervalSec_ = sm.strobeIntervalSec_;
  queueLen_          = sm.queueLen_;
  strobeThreadId_    = sm.strobeThreadId_;
  noOp_              = sm.noOp_;
  
  counterMap_        = sm.counterMap_;
  
  outputOrder_       = sm.outputOrder_;
  outputDivisors_    = sm.outputDivisors_;
  outputLabels_      = sm.outputLabels_;
  outputUnits_       = sm.outputUnits_;
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

bool StatManager::hasCounter(std::string name)
{
  bool retVal = false;

  try {

    lock();

    retVal = counterMap_.find(name) != counterMap_.end();

  } catch(...) {
  }

  unlock();

  return retVal;
}

bool StatManager::hasCountersContaining(std::string name)
{
  bool retVal = false;

  try {

    lock();

    for(std::map<std::string, Counter>::iterator countIter=counterMap_.begin(); countIter != counterMap_.end(); countIter++) {

      std::string::size_type idx = countIter->first.find(name);
      
      if(idx != std::string::npos)
	retVal = true;
    }

  } catch(...) {
  }

  unlock();

  return retVal;
}

void StatManager::initCounter(std::string name)
{
  std::map<std::string, uint64_t> addMap;
  addMap[name] = 0;
  add(addMap);
}

void StatManager::initCounter(std::string name, std::string label, std::string unit, unsigned divisor)
{
  initCounter(name);
  setOutputLabel(name, label);
  setOutputUnit(name, unit);
  setOutputDivisor(name, divisor);
}

void StatManager::initCounter(std::string name, volatile uint64_t* valPtr)
{
  TRYLOCK(

	  // Only resize the buffer if the counter doesn't already exist

	  if(counterMap_.find(name)==counterMap_.end()) {
	    Counter& counter = counterMap_[name];
	    counter.resize(queueLen_);
	  }

	  counterMap_[name].extIntegratedCurrPtr_ = valPtr;

	  )
}

void StatManager::initCounter(std::string name, volatile uint64_t* valPtr, std::string label, std::string unit, unsigned divisor)
{
  initCounter(name, valPtr);
  setOutputLabel(name, label);
  setOutputUnit(name, unit);
  setOutputDivisor(name, divisor);
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
 * Add to a counter
 */
void StatManager::add(std::string name, uint64_t val)
{
  std::map<std::string, uint64_t> addMap;
  addMap[name] = val;
  add(addMap);
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

void StatManager::getCountsContaining(std::map<std::string, Sample>& sampleMap,
				      unsigned nSample)
{
  if(noOp_)
    return;

  try {
    lock();

    std::map<std::string, Sample> tmpMap;

    for(std::map<std::string, Sample>::iterator sampIter=sampleMap.begin(); sampIter != sampleMap.end(); sampIter++) {
      std::string key = sampIter->first;
      for(std::map<std::string, Counter>::iterator countIter=counterMap_.begin(); countIter != counterMap_.end(); countIter++) {
	
	std::string::size_type idx = countIter->first.find(key);
	if(idx != std::string::npos) {
	  tmpMap[countIter->first];
	}
      }
    }
    
    unlock();

    getCounts(tmpMap, nSample);
    sampleMap = tmpMap;

  } catch(...) {
    unlock();
  }

}

/**.......................................................................
 * Iterate through the map of requested counters, filling in data
 * where available
 */
void StatManager::getAllCounts(std::map<std::string, Sample>& sampleMap,
			       unsigned nSample) 
{
  TRYLOCK(

	  for(std::map<std::string, Counter>::iterator iter=counterMap_.begin(); iter != counterMap_.end(); iter++) {

	    Counter& counter = iter->second;
	    Sample&  sample  = sampleMap[iter->first];

	    counter.getCounts(sample, nSample);
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

void StatManager::setOutputDivisor(std::string name, double divisor)
{
  outputDivisors_[name] = divisor;
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

void StatManager::setOutputLabel(std::string name, std::string label)
{
  outputLabels_[name] = label;
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

void StatManager::setOutputUnit(std::string name, std::string units)
{
  outputUnits_[name] = units;
}

std::string StatManager::getUnits(std::string name)
{
  if(outputUnits_.find(name) != outputUnits_.end()) {
    return outputUnits_[name];
  } else {
    return "#";
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
  }

  return maxLen;
}

/**.......................................................................
 * Return the longest time header we will encounter
 */
unsigned StatManager::longestTimeHeader(std::map<std::string, Sample>& sampleMap)
{
  bool first=true;
  size_t maxLen=0;

  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    Sample& sample = iter->second;

    if(sample.differentials_.size() > 0) {
      uint64_t time = sample.differentials_[sample.differentials_.size()-1].first;
      size_t timeLen = formattedDate(time).size();

      if(first) {
	first = false;
	maxLen = timeLen;
      } else
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
  size_t timeLen    = longestTimeHeader(sampleMap) + 2;
  size_t headerLen  = longestHeader(sampleMap) + 2;
  size_t lineLen    = timeLen + nCounter * headerLen;

  unsigned nCol = getNCol();

  if(nCol < lineLen) {

    unsigned nCounterPerTable = (nCol - timeLen) / headerLen;
    lineLen = timeLen + nCounterPerTable * headerLen;
    unsigned nTable = ceil((double)(nCounter) / nCounterPerTable);

    std::ostringstream os;

    unsigned iCounter = 0;

    std::map<std::string, Sample> tmpMap;
    unsigned iTable=0;
    for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); iter++, iCounter++) {

      tmpMap[iter->first] = iter->second;

      if((iCounter+1) % nCounterPerTable == 0 || iCounter+1 == nCounter) {
	++iTable;
	os << formatTable(header, tmpMap, lineLen, timeLen, headerLen, iTable, nTable);
	tmpMap.clear();
      }
    }

    return os.str();

  } else {
    return formatTable(header, sampleMap, lineLen, timeLen, headerLen, 1, 1);
  }
}
  
/**.......................................................................
 * Format a single table of output
 */
std::string StatManager::formatTable(std::string header,  std::map<std::string, Sample>& sampleMap, 
				     unsigned lineLen, unsigned timeLen, unsigned headerLen, unsigned iTable, unsigned nTable) 
{
  std::ostringstream os;

  // Print a line of '='

  for(unsigned i=0; i < lineLen; i++)
    os << "=";
  os << std::endl;

  // Print the header

  std::ostringstream osHead;
  osHead << header;

  if(nTable > 1)
    osHead << " (" << iTable << " of " << nTable << ")";

  os << std::setw(lineLen) << std::left << osHead.str() << std::endl;

  // Print a line of '-'

  for(unsigned i=0; i < lineLen; i++)
    os << "-";
  os << std::endl;

  // Allocate a space for the timestamps

  os << std::setw(timeLen) << " ";

  // Now print the rest of the headers

  for(std::map<std::string, Sample>::iterator iter=sampleMap.begin(); iter != sampleMap.end(); 
      iter++) {
    os << std::setw(headerLen) << std::left << getHeader(iter->first);
  }

  os << std::endl;
    
  os << "Totals";
  for(unsigned i=0; i < lineLen-strlen("Totals"); i++)
    os << "-";
  os << std::endl << std::endl;

  os << std::setw(timeLen) << " ";
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
      
    os << std::setw(timeLen) << std::left << formattedDate(maxSample.differentials_[i].first);

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

void StatManager::startTimer()
{
  timer_.start();
}

void StatManager::stopTimer()
{
  timer_.stop();
}

uint64_t StatManager::elapsedMicroSeconds()
{
  return timer_.deltaMicroSeconds();
}

//=======================================================================
// Methods of StatManager::Counter
//=======================================================================

StatManager::Counter::Counter()
{
  extIntegratedCurrPtr_ = 0;
  integratedCurr_       = 0;
  integratedLast_       = 0;
  first_ = true;
  resize(0);
}

StatManager::Counter::Counter(const Counter& count)
{
  *this = count;
}

StatManager::Counter::Counter(Counter& count)
{
  *this = count;
}

void StatManager::Counter::operator=(const Counter& count)
{
  *this = (Counter&) count;
}

void StatManager::Counter::operator=(Counter& count)
{
  bool first_ = count.first_;
  
  extIntegratedCurrPtr_ = count.extIntegratedCurrPtr_;
  integratedCurr_ = count.integratedCurr_;
  integratedLast_ = count.integratedLast_;
  
  differentials_  = count.differentials_;
  sampleTimes_    = count.sampleTimes_;
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
  
  // By definition, we can't store per-unit-time differentials until
  // the second strobe, but I'm going to store them anyway, taking it
  // to mean 'whatever happened during the previous interval'. set #if 1
  // to #if 0 if you don't like this behavior

  if(first_) {
    first_ = false;
#if 1
    if(extIntegratedCurrPtr_)
      diff = *extIntegratedCurrPtr_ - integratedLast_;
    else
      diff = integratedCurr_ - integratedLast_;

    differentials_.push(diff);
    sampleTimes_.push(time);
#endif
  } else {

    if(extIntegratedCurrPtr_)
      diff = *extIntegratedCurrPtr_ - integratedLast_;
    else
      diff = integratedCurr_ - integratedLast_;

    differentials_.push(diff);
    sampleTimes_.push(time);
  }

  integratedLast_ = extIntegratedCurrPtr_ ? *extIntegratedCurrPtr_ : integratedCurr_;
}

/**.......................................................................
 * Retrieve the last nSample differential counts for this counter
 * (most recent will be the first element in the returned arrays)
 */
void StatManager::Counter::getCounts(Sample& sample,
				     unsigned nSample)
{
  sample.total_ = extIntegratedCurrPtr_ ? *extIntegratedCurrPtr_ : integratedCurr_;

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

unsigned StatManager::getNCol()
{
  struct winsize ws;
  ioctl(0, TIOCGWINSZ, &ws);
  return ws.ws_col;
}
