#ifndef LEVELDB_UTIL_TIMER_H
#define LEVELDB_UTIL_TIMER_H

/**
 * @file Timer.h
 * 
 * Tagged: Tue Jun  2 08:51:01 PDT 2015
 */
#include <stdint.h>
#include <sys/time.h>

namespace leveldb {
  namespace util {

    class Timer {
    public:

      /**
       * Constructor.
       */
      Timer();

      /**
       * Destructor.
       */
      virtual ~Timer();

      // Return the current time, as microseconds
      
      uint64_t currentMicroSeconds();

      // Start/stop and return elapsed time

      void start();
      void stop();
      uint64_t deltaMicroSeconds();

    private:

      uint64_t startMs_;
      uint64_t stopMs_;
      uint64_t diffMs_;
      uint64_t integratedMs_;

    }; // End class Timer

  } // End namespace util
} // End namespace leveldb


#endif // End #ifndef LEVELDB_UTIL_TIMER_H
