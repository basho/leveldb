#ifndef LEVELDB_UTIL_IOLOCK_H
#define LEVELDB_UTIL_IOLOCK_H

/**
 * @file IoLock.h
 * 
 * Simple class for managing interleaved writes to stdout and stderr
 * from multiple threads.  Used for debugging
 */
#include "port/port.h"

namespace leveldb {
  namespace util {
    
    class IoLock {
    public:
      
      /**
       * Destructor.
       */
      virtual ~IoLock();
      
      static void lockCout();
      static void unlockCout();
      static void lockCerr();
      static void unlockCerr();

    private:

      static port::Mutex coutMutex_;
      static port::Mutex cerrMutex_;

      /**
       * Private constructor prevents instantiation
       */
      IoLock();

    }; // End class IoLock
    
  } // End namespace util
} // End namespace leveldb



#endif // End #ifndef LEVELDB_UTIL_IOLOCK_H
