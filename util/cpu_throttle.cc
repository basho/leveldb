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

#include <pthread.h>
#include <sched.h>  // for linux sched_getaffinity
#include <unistd.h>

#include "cpu_throttle.h"

namespace leveldb {

CpuThrottler *  CpuThrottle::inst = 0;

void CpuThrottle::init(unsigned max_cpu_percent)
{
    if (! inst)
        inst = new CpuThrottler(max_cpu_percent);
}

CpuThrottler::CpuThrottler(unsigned max_cpu_percent)
    : cpus_used(0)
    , cpus_max(calc_cpu_max(max_cpu_percent))
{
    std::memset(& sync, 0, sizeof(sync));
    std::memset(& cond, 0, sizeof(cond));

    int rc;
    if ((rc = pthread_mutex_init(& sync, 0)) != 0)
        fatal_error((__LINE__ - 1), "pthread_mutex_init", rc);
    if ((rc = pthread_cond_init(& cond, 0)) != 0)
        fatal_error((__LINE__ - 1), "pthread_cond_init", rc);
}

CpuThrottler::~CpuThrottler()
{
    pthread_cond_destroy(& cond);
    pthread_mutex_destroy(& sync);
}

bool CpuThrottler::acquire()
{
    int rc;
    if ((rc = pthread_mutex_lock(& sync)) != 0)
        fatal_error((__LINE__ - 1), "pthread_mutex_lock", rc);
    ++n_waiting;

    while (cpus_used >= cpus_max)
        if ((rc = pthread_cond_wait(& cond, & sync)) != 0)
            fatal_error((__LINE__ - 1), "pthread_cond_wait", rc);
    ++cpus_used;

    --n_waiting;
    if ((rc = pthread_mutex_unlock(& sync)) != 0)
        fatal_error((__LINE__ - 1), "pthread_mutex_unlock", rc);

    return  true;
}

void CpuThrottler::release()
{
    int rc;
    if ((rc = pthread_mutex_lock(& sync)) != 0)
        fatal_error((__LINE__ - 1), "pthread_mutex_lock", rc);

    --cpus_used;
    //
    //  Don't rely on 'n_waiting', let the system decide whether anyone
    //  needs to be woken up.
    //
    if ((rc = pthread_cond_broadcast(& cond)) != 0)
        fatal_error((__LINE__ - 1), "pthread_cond_broadcast", rc);

    if ((rc = pthread_mutex_unlock(& sync)) != 0)
        fatal_error((__LINE__ - 1), "pthread_mutex_unlock", rc);
}

unsigned CpuThrottler::calc_cpu_max(unsigned max_cpu_percent)
{
    unsigned    ncpus, ucpus;

#if     defined(_SC_NPROCESSORS_CONF)
    long    cpus = sysconf(_SC_NPROCESSORS_CONF);
    if (cpus < 0)
        fatal_error((__LINE__ - 2), "sysconf", errno);
    ncpus = (unsigned) cpus;
#elif   defined(_SC_NPROCESSORS_ONLN)
    long    cpus = sysconf(_SC_NPROCESSORS_ONLN);
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

    std::printf(
        "CpuThrottler::calc_cpu_max(%u) ==> %u CPUs, using up to %u\n",
        max_cpu_percent, ncpus, ucpus);

    return  ucpus;
}

//  exit or throw an exception, must not return
void CpuThrottler::fatal_error(unsigned line, const char * func, int err)
{
    std::fprintf(stderr,
        "CpuThrottle:%u: error from %s: %s\n",
        line, func, std::strerror(err));
    std::exit(err ? err : -1);
}

}   //  namespace leveldb
