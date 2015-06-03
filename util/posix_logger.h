// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include "leveldb/env.h"
#include "util/mutexlock.h"
#include "util/classes/StatManager.h"

namespace leveldb {

class PosixLogger : public Logger {
 private:
  FILE* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
  leveldb::util::StatManager statManager_;

 public:
  PosixLogger(FILE* f, uint64_t (*gettid)()) : file_(f), gettid_(gettid), statManager_(1, 10, false) 
  { 
    statManager_.initCounter("logWriteMicros");
    statManager_.initCounter("logWriteNbyte");
    statManager_.initCounter("logNWrites");

    statManager_.spawnStrobeThread();
  }

  virtual ~PosixLogger() {
    fclose(file_);
  }
  virtual void Logv(const char* format, va_list ap) {

    //------------------------------------------------------------
    // Start the stat timer now
    //------------------------------------------------------------

    statManager_.timer_.start();

    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (iter == 0) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, NULL);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (iter == 0) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || p[-1] != '\n') {
        *p++ = '\n';
      }

      assert(p <= limit);
      fwrite(base, 1, p - base, file_);
      fflush(file_);

      //------------------------------------------------------------
      // Stop the stat timer and store logging stats
      //------------------------------------------------------------

      statManager_.timer_.stop();
      std::map<std::string, uint64_t> addMap;

      addMap["logWriteMicros"] = statManager_.timer_.deltaMicroSeconds();
      addMap["logWriteNbyte"]  = p-base;
      addMap["logNWrites"]     = 1;

      statManager_.add(addMap);

      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }

  //------------------------------------------------------------
  // Overloaded interface method from StatBase to return counts for a
  // set of items
  //------------------------------------------------------------

  virtual void getCounts(std::map<std::string, leveldb::util::Sample>& sampleMap,
			 unsigned nSample=0) {
    return statManager_.getCounts(sampleMap, nSample);
  }

  virtual std::string formatOutput(std::string header, std::map<std::string, leveldb::util::Sample>& sampleMap) {
    return statManager_.formatOutput(header, sampleMap);
  }

  virtual void setOutputOrder(std::vector<std::string>& order) {
    return statManager_.setOutputOrder(order);
  }

  virtual void setOutputDivisors(std::map<std::string, double>& divisorMap) {
    return statManager_.setOutputDivisors(divisorMap);
  }

  virtual void setOutputLabels(std::map<std::string, std::string>& labelMap) {
    return statManager_.setOutputLabels(labelMap);
  }

  virtual void setOutputUnits(std::map<std::string, std::string>& unitMap) {
    return statManager_.setOutputUnits(unitMap);
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
