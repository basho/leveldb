#ifndef LEVELDB_UTIL_STATMANAGER_H
#define LEVELDB_UTIL_STATMANAGER_H

/**
 * @file StatManager.h
 * 
 * A class for managing an arbitrary set of counter statistics
 */
#include "leveldb/perf_count.h"
#include "port/port.h"
#include "util/classes/CircBuf.h"
#include "util/classes/Timer.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#define THREAD_START(fn) void* (fn)(void *arg)

namespace leveldb {
  namespace util {

    class StatManager {
    public:

      //------------------------------------------------------------
      // An object that will manage stats for a single counter
      //------------------------------------------------------------

      struct Counter {

	// Default constructor

	Counter();

	// Resize the circular buffer of samples to queueLen

	void resize(unsigned queueLen);

	// Add to this counter

	void add(uint64_t val);

	// Store differential counts

	void storeDifferential(uint64_t time);

	// Retrieve total and up to nSample differential counts (all if nSample == 0)

	void getCounts(Sample& samples,
		       unsigned nSample=0);

	bool first_;

	uint64_t integratedCurr_;
	uint64_t integratedLast_;

	CircBuf<uint64_t> differentials_;
	CircBuf<uint64_t> sampleTimes_;
      };

      /**
       * Constructor.
       */
      StatManager();
      StatManager(unsigned strobeIntervalSec, unsigned queueLen, bool noOp=false);

      /**
       * Destructor.
       */
      virtual ~StatManager();

      void initialize(unsigned strobeIntervalSec, unsigned queueLen, bool noOp);

      //------------------------------------------------------------
      // Initialize a new counter
      //------------------------------------------------------------

      void initCounter(std::string name);

      //------------------------------------------------------------
      // Add to the counters maintained by this object
      //------------------------------------------------------------

      void add(std::map<std::string, uint64_t>& counterMap);

      //------------------------------------------------------------
      // Store differential counts
      //------------------------------------------------------------

      void strobe(uint64_t time);

      //------------------------------------------------------------
      // Retrieve a map of up to nSample counter samples from this
      // object (all if nSample==0)
      //------------------------------------------------------------

      void getCounts(std::map<std::string, Sample>& sampleMap,
		     unsigned nSample=0);

      //------------------------------------------------------------
      // Spawn the thread that will strobe our counters.  To strobe 
      // counters from an external thread, simply don't spawn this
      //------------------------------------------------------------

      void spawnStrobeThread();

      // Convenience methods for formatting output

      void setOutputOrder(std::vector<std::string>& order);
      void setOutputDivisors(std::map<std::string, double>& divisorMap);
      void setOutputLabels(std::map<std::string, std::string>& labelMap);
      void setOutputUnits(std::map<std::string, std::string>& unitMap);

      // Format output for the requested set of samples

      std::string formatOutput(std::string header, 
			       std::map<std::string, Sample>& sampleMap);

      Timer timer_;

    private:

      void lock();
      void unlock();

      // A startup function for the strobe thread.

      static THREAD_START(startStrobeThread);
      void* runStrobeThread(void* arg);

      //------------------------------------------------------------
      // Utility methods for formatting output
      //------------------------------------------------------------

      double getDivisor(std::string name);
      std::string getLabel(std::string name);
      std::string getUnits(std::string name);
      std::string getHeader(std::string name);

      std::string formattedDate(uint64_t sec);

      unsigned longestHeader(std::map<std::string, Sample>& sampleMap);
      std::string longestCounter(std::map<std::string, Sample>& sampleMap);

      //------------------------------------------------------------
      // Utility members for formatting output
      //------------------------------------------------------------

      std::vector<std::string>           outputOrder_;
      std::map<std::string, double>      outputDivisors_;
      std::map<std::string, std::string> outputLabels_;
      std::map<std::string, std::string> outputUnits_;

      //------------------------------------------------------------
      // A map of counters maintained by this object
      //------------------------------------------------------------

      std::map<std::string, Counter> counterMap_;

      //------------------------------------------------------------
      // Strobe thread management
      //------------------------------------------------------------

      pthread_t strobeThreadId_;
      unsigned  strobeIntervalSec_;

      port::Mutex mutex_;

      // Length of the counter queues managed by this object

      unsigned queueLen_;

      // True to make this class a no-op (for testing)

      bool noOp_;

    }; // End class StatManager
  } // End namespace util
} // End namespace leveldb



#endif // End #ifndef LEVELDB_UTIL_STATMANAGER_H
