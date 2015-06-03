#include <iostream>

#include "util/classes/IoLock.h"

using namespace leveldb::util;

/**.......................................................................
 * Constructor.
 */
IoLock::IoLock() {}

// Initialize static variables

leveldb::port::Mutex IoLock::coutMutex_;
leveldb::port::Mutex IoLock::cerrMutex_;

/**.......................................................................
 * Destructor.
 */
IoLock::~IoLock() {}

void IoLock::lockCout()
{
  coutMutex_.Lock();
}

void IoLock::unlockCout()
{
  coutMutex_.Unlock();
}

void IoLock::lockCerr()
{
  cerrMutex_.Lock();
}

void IoLock::unlockCerr()
{
  cerrMutex_.Unlock();
}
