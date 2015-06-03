#ifndef LEVELDB_UTIL_EXCEPTIONUTILS_H
#define LEVELDB_UTIL_EXCEPTIONUTILS_H

#include <errno.h>
#include <iostream>
#include <stdexcept>
#include <sstream>

#include "util/classes/IoLock.h"

#ifdef ThrowError
#undef ThrowError
#endif

#define ThrowError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text;\
  throw std::runtime_error(_macroOs.str());\
}

#ifdef ThrowSysError
#undef ThrowSysError
#endif

#define ThrowSysError(text) \
{\
  std::ostringstream _macroOs; \
  _macroOs << text << ": " << strerror(errno);\
  throw std::runtime_error(_macroOs.str());\
}

#ifdef COUT
#undef COUT
#endif

#define COUT(statement) \
{\
  std::cout << statement << std::endl;\
}
#endif

#ifdef IOCOUT
#undef IOCOUT
#endif

#define IOCOUT(statement) \
{\
    leveldb::util::IoLock::lockCout(); \
    std::cout << statement << std::endl; \
    leveldb::util::IoLock::unlockCout(); \
}

#ifdef IOCERR
#undef IOCERR
#endif

#define IOCERR(statement) \
{\
    leveldb::util::IoLock::lockCerr(); \
    std::cerr << statement << std::endl; \
    leveldb::util::IoLock::unlockCerr(); \
}


