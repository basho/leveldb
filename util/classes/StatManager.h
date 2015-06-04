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

    class StatManager : public StatManagerBase {
    public:

      //------------------------------------------------------------
      // An object that will manage stats for a single counter
      //------------------------------------------------------------

      struct Counter {

	// Default constructor

	Counter();
	Counter(const Counter& count);
	Counter(Counter& count);
	void operator=(const Counter& count);
	void operator=(Counter& count);

	// Resize the circular buffer of samples to queueLen

	void resize(unsigned queueLen);

	// Add to this counter

	void add(uint64_t val);

	// Store differential counts

	void storeDifferential(uint64_t time);

	// Retrieve total and up to nSample differential counts (all
	// if nSample == 0)

	void getCounts(Sample& samples,
		       unsigned nSample=0);

	bool first_;

	volatile uint64_t* extIntegratedCurrPtr_;
	uint64_t integratedCurr_;
	uint64_t integratedLast_;

	CircBuf<uint64_t> differentials_;
	CircBuf<uint64_t> sampleTimes_;
      };

      /**
       * Constructor.
       */
      StatManager();
      StatManager(const StatManager& sm);
      StatManager(StatManager& sm);
      void operator=(const StatManager& sm);
      void operator=(StatManager& sm);
      StatManager(unsigned strobeIntervalSec, unsigned queueLen, bool noOp=false);

      /**
       * Destructor.
       */
      virtual ~StatManager();

      void initialize(unsigned strobeIntervalSec, unsigned queueLen, bool noOp);

      //------------------------------------------------------------
      // Return true if this object contains the named counter
      //------------------------------------------------------------

      bool hasCounter(std::string name);

      //------------------------------------------------------------
      // Return true if this object has counters containing the
      // substring name
      //------------------------------------------------------------

      bool hasCountersContaining(std::string name);

      //------------------------------------------------------------
      // Initialize new counters.
      // 
      // add() will do this for you if adding
      // counters that haven't already been initialized
      //------------------------------------------------------------

      // Initialize a new internal counter

      void initCounter(std::string name);
      void initCounter(std::string name, std::string label, std::string unit, unsigned divisor);

      // Initialize a counter whose count is stored in an external pointer

      void initCounter(std::string name, volatile uint64_t* valPtr);
      void initCounter(std::string name, volatile uint64_t* valPtr, std::string label, std::string unit, unsigned divisor);

      //------------------------------------------------------------
      // Add to the counters maintained by this object
      //------------------------------------------------------------

      void add(std::map<std::string, uint64_t>& counterMap);
      void add(std::string name, uint64_t val);

      //------------------------------------------------------------
      // Store differential counts
      //------------------------------------------------------------

      void strobe(uint64_t time);

      //------------------------------------------------------------
      // Retrieve a map of up to nSample counter samples from this
      // object (all if nSample==0). 
      //
      // Returns counts for counters matching the keys in sampleMap
      //------------------------------------------------------------

      void getCounts(std::map<std::string, Sample>& sampleMap,
		     unsigned nSample=0);

      //------------------------------------------------------------
      // Retrieve counts for counters containing the keys in sampleMap
      //------------------------------------------------------------

      void getCountsContaining(std::map<std::string, Sample>& sampleMap,
			       unsigned nSample=0);

      //------------------------------------------------------------
      // Retrieve counts for all counters managed by this object
      //------------------------------------------------------------

      void getAllCounts(std::map<std::string, Sample>& sampleMap,
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

      void setOutputDivisor(std::string name, double divisor);
      void setOutputLabel(std::string name, std::string label);
      void setOutputUnit(std::string name, std::string unit);

      // Format output for the requested set of samples

      std::string formatOutput(std::string header, 
			       std::map<std::string, Sample>& sampleMap);

      void startTimer();
      void stopTimer();
      uint64_t elapsedMicroSeconds();

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

      std::string formatTable(std::string header,  std::map<std::string, Sample>& sampleMap, 
			      unsigned lineLen, unsigned timeLen, unsigned headerLen, unsigned iTable, unsigned nTable);

      unsigned longestHeader(std::map<std::string, Sample>& sampleMap);
      unsigned longestTimeHeader(std::map<std::string, Sample>& sampleMap);
      std::string longestCounter(std::map<std::string, Sample>& sampleMap);

      unsigned getNCol();

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

      Timer timer_;

      // Length of the counter queues managed by this object

      unsigned queueLen_;

      // True to make this class a no-op (for testing)

      bool noOp_;

    }; // End class StatManager
  } // End namespace util
} // End namespace leveldb



#endif // End #ifndef LEVELDB_UTIL_STATMANAGER_H
