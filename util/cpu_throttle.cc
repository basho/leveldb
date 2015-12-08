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

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

// for linux sched_getaffinity
#ifdef  __linux__
#include <sched.h>
#endif

//  port.h will pull in unistd.h
#include "cpu_throttle.h"

namespace leveldb {

namespace {

//  exit or throw an exception, must not return
void fatal_error(unsigned line, const char * func, int err)
{
    std::fprintf(stderr,
        "CpuThrottle:%u: error from %s: %s\n",
        line, func, std::strerror(err));
    std::exit(err ? err : -1);
}

unsigned calc_cpu_max(unsigned max_cpu_percent)
{
    //  distinct variables, let the compiler optimize them out
    unsigned    ncpus, ucpus;

#if     defined(_SC_NPROCESSORS_CONF)
    const long  cpus = ::sysconf(_SC_NPROCESSORS_CONF);
    if (cpus < 0)
        fatal_error((__LINE__ - 2), "sysconf", errno);
    ncpus = (unsigned) cpus;
#elif   defined(_SC_NPROCESSORS_ONLN)
    const long  cpus = ::sysconf(_SC_NPROCESSORS_ONLN);
    if (cpus < 0)
        fatal_error((__LINE__ - 2), "sysconf", errno);
    ncpus = (unsigned) cpus;
#elif   defined(__linux__)
    cpu_set_t   cpus;
    CPU_ZERO(& cpus);
    if (sched_getaffinity(0, sizeof(cpus), & cpus))
        fatal_error((__LINE__ - 1), "sched_getaffinity", errno);
    ncpus = 0;
    for (unsigned x = 0; x < CPU_SETSIZE; ++x)
        if (CPU_ISSET(x, & cpus))
            ++ncpus;
#else
#error  No clause to get CPU count!
#endif

    if (max_cpu_percent == 0 || ncpus < 2)
        ucpus = 1;
    else if (max_cpu_percent > 99)
        ucpus = ncpus;
    else
        ucpus = std::max(1u, ((ncpus * max_cpu_percent) / 100));

#if CPU_THROTTLER_DEBUG
    std::printf(
        "CpuThrottler:calc_cpu_max(%u) ==> %u CPUs, using up to %u\n",
        max_cpu_percent, ncpus, ucpus);
#endif
    return  ucpus;
}
    
}   //  anonymous namespace

CpuThrottler *  CpuThrottle::inst = 0;

void CpuThrottle::init(unsigned max_cpu_percent)
{
    if (! inst)
        inst = new CpuThrottler(max_cpu_percent);
}

CpuThrottler::CpuThrottler(unsigned max_cpu_percent)
    : cpus_max(calc_cpu_max(max_cpu_percent))
    , cpus_used(0)
#if CPU_THROTTLER_STATS
    , n_waiting(0)
#endif
    , sync()
    , cond(& sync)
{
}

CpuThrottler::~CpuThrottler()
{
}

bool CpuThrottler::acquire()
{
    sync.Lock();
#if CPU_THROTTLER_STATS
    ++n_waiting;
#endif
    while (cpus_used >= cpus_max)
        cond.Wait();
    ++cpus_used;
#if CPU_THROTTLER_STATS
    --n_waiting;
#endif
    sync.Unlock();
    return  true;
}

bool CpuThrottler::release()
{
    sync.Lock();
    --cpus_used;
    //
    //  Don't rely on 'n_waiting' even if it's present - let the system
    //  decide whether anyone needs to be woken up.
    //  Wake up everyone, which should be a small set, to make sure somebody
    //  gets moving.
    //
    cond.SignalAll();
    sync.Unlock();
    return  false;
}

}   //  namespace leveldb
