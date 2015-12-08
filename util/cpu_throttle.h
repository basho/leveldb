//
// Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#ifndef STORAGE_LEVELDB_CPU_THROTTLE_H_
#define STORAGE_LEVELDB_CPU_THROTTLE_H_ 1

#include "port/port.h"

#define CPU_THROTTLER_STATS 1
#define CPU_THROTTLER_DEBUG 1

namespace leveldb {

class CpuThrottler
{
public:

    CpuThrottler(unsigned max_cpu_percent);
    ~CpuThrottler();

    //  These return the value the resulting 'held' status
    bool    acquire();
    bool    release();

#if CPU_THROTTLER_STATS
    inline  unsigned    running() { return cpus_used; }
    inline  unsigned    waiters() { return n_waiting; }
#endif

private:

    unsigned      const cpus_max;
    unsigned   volatile cpus_used;
#if CPU_THROTTLER_STATS
    unsigned   volatile n_waiting;
#endif
    ::leveldb::port::Mutex      sync;
    ::leveldb::port::CondVar    cond;

    //  prohibit copies
    CpuThrottler(const CpuThrottler &);
    CpuThrottler & operator = (const CpuThrottler &);
};

class CpuThrottle
{
public:

    inline  void  acquire() { if (! held) held = inst->acquire(); }
    inline  void  release() { if (held) held = inst->release(); }

    inline  CpuThrottle(bool hold) : held(false) { if (hold) acquire(); }
    inline  CpuThrottle() : held(false) { acquire(); }
    inline ~CpuThrottle() { release(); }
    //
    //  Initialize to a reasonable number of in-use CPUs based on a specified
    //  percentage from 1 to 100.  Silly values will be adjusted accordingly.
    //  In all cases, at least one CPU will be used.
    //
    static  void  init(unsigned max_cpu_percent);
#if CPU_THROTTLER_STATS
    static  inline  unsigned    running()   { return inst->running(); }
    static  inline  unsigned    waiters()   { return inst->waiters(); }
#endif

private:

    bool    held;
    static  CpuThrottler *  inst;

    //  prohibit copies
    CpuThrottle(const CpuThrottle &);
    CpuThrottle & operator = (const CpuThrottle &);
};

}   //  namespace leveldb

#endif  //  STORAGE_LEVELDB_CPU_THROTTLE_H_
