#include "util/classes/Timer.h"

using namespace leveldb::util;

/**.......................................................................
 * Constructor.
 */
Timer::Timer() 
{
  startMs_      = 0;
  stopMs_       = 0;
  diffMs_       = 0;
  integratedMs_ = 0;
}

/**.......................................................................
 * Destructor.
 */
Timer::~Timer() {}

void Timer::start()
{
  startMs_ = currentMicroSeconds();
}

void Timer::stop()
{
  stopMs_        = currentMicroSeconds();
  diffMs_        = stopMs_ - startMs_;
  integratedMs_ += diffMs_;
}

/**.......................................................................
 * Return the elapsed inteval, as microseconds
 */
uint64_t Timer::deltaMicroSeconds()
{
  return diffMs_;
}

/**.......................................................................
 * Return the current time, as microseconds
 */
uint64_t Timer::currentMicroSeconds() 
{
#if _POSIX_TIMERS >= 200801L

  struct timespec ts;
  
  // this is rumored to be faster that gettimeofday(), and sometimes
  // shift less ... someday use CLOCK_MONOTONIC_RAW

  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec/1000;

#else

  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;

#endif
}

