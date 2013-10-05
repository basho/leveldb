// -------------------------------------------------------------------
//
// throttle.cc
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
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
// -------------------------------------------------------------------

#include <sys/time.h>

#include "leveldb/perf_count.h"
#include "leveldb/env.h"

#include "db/db_impl.h"
#include "util/cache2.h"
#include "util/db_list.h"
#include "util/flexcache.h"
#include "util/throttle.h"

namespace leveldb {

pthread_rwlock_t gThreadLock0;
pthread_rwlock_t gThreadLock1;

pthread_mutex_t gThrottleMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gThrottleCond = PTHREAD_COND_INITIALIZER;

#define THROTTLE_SECONDS 60
#define THROTTLE_TIME THROTTLE_SECONDS*1000000
#define THROTTLE_SCALING 17
#define THROTTLE_INTERVALS 63

struct ThrottleData_t
{
    uint64_t m_Micros;
    uint64_t m_Keys;
    uint64_t m_Backlog;
    uint64_t m_Compactions;
};

ThrottleData_t gThrottleData[THROTTLE_INTERVALS];

uint64_t gThrottleRate;

static bool gThrottleRunning=false;
static pthread_t gThrottleThreadId;

static void * ThrottleThread(void * arg);


void
ThrottleInit()
{

    pthread_rwlock_init(&gThreadLock0, NULL);
    pthread_rwlock_init(&gThreadLock1, NULL);

    memset(&gThrottleData, 0, sizeof(gThrottleData));
    gThrottleRate=0;

    pthread_create(&gThrottleThreadId, NULL,  &ThrottleThread, NULL);

    return;

}   // ThrottleInit


static void *
ThrottleThread(
    void * /*arg*/)
{
    uint64_t tot_micros, tot_keys, tot_backlog, tot_compact;
    int replace_idx, loop;
    uint64_t new_throttle;
    time_t now_seconds, cache_expire;
    struct timespec wait_time;

    replace_idx=2;
    gThrottleRunning=true;
    now_seconds=0;
    cache_expire=0;

    while(gThrottleRunning)
    {
        pthread_mutex_lock(&gThrottleMutex);

        // sleep 1 minute
#if _POSIX_TIMERS >= 200801L
        clock_gettime(CLOCK_REALTIME, &wait_time);
#else
        struct timeval tv;
        gettimeofday(&tv, NULL);
        wait_time.tv_sec=tv.tv_sec;
        wait_time.tv_nsec=tv.tv_usec*1000;
#endif

        now_seconds=wait_time.tv_sec;
        wait_time.tv_sec+=THROTTLE_SECONDS;
        pthread_cond_timedwait(&gThrottleCond, &gThrottleMutex,
                               &wait_time);
        gThrottleData[replace_idx]=gThrottleData[1];
        memset(&gThrottleData[1], 0, sizeof(gThrottleData[1]));
        pthread_mutex_unlock(&gThrottleMutex);

        tot_micros=0;
        tot_keys=0;
        tot_backlog=0;
        tot_compact=0;

        // this could be faster by keeping running totals and
        //  subtracting [replace_idx] before copying [0] into it,
        //  then adding new [replace_idx].  But that needs more
        //  time for testing.
        for (loop=2; loop<THROTTLE_INTERVALS; ++loop)
        {
            tot_micros+=gThrottleData[loop].m_Micros;
            tot_keys+=gThrottleData[loop].m_Keys;
            tot_backlog+=gThrottleData[loop].m_Backlog;
            tot_compact+=gThrottleData[loop].m_Compactions;
        }   // for

	// non-level0 data available?
        if (0!=tot_keys)
        {
            if (0==tot_compact)
                tot_compact=1;

            // average write time for level 1+ compactions per key
            //   times the average number of tasks waiting
            //   ( the *100 stuff is to exploit fractional data in integers )
            new_throttle=((tot_micros*100) / tot_keys)
                * ((tot_backlog*100) / tot_compact);

            new_throttle /= 10000;  // remove *100 stuff
            if (0==new_throttle)
                new_throttle=1;     // throttle must have an effect
        }   // if

	// attempt to most recent level0
	//  (only use most recent level0 until level1+ data becomes available,
	//   useful on restart of heavily loaded server)
	else if (0!=gThrottleData[0].m_Keys && 0!=gThrottleData[0].m_Compactions)
	{
            pthread_mutex_lock(&gThrottleMutex);
            new_throttle=(gThrottleData[0].m_Micros / gThrottleData[0].m_Keys)
	      * (gThrottleData[0].m_Backlog / gThrottleData[0].m_Compactions);
            pthread_mutex_unlock(&gThrottleMutex);
	}   // else if
        else
        {
            new_throttle=1;
        }   // else

        // change the throttle slowly
        if (gThrottleRate < new_throttle)
            gThrottleRate+=(new_throttle - gThrottleRate)/THROTTLE_SCALING;
        else
            gThrottleRate-=(gThrottleRate - new_throttle)/THROTTLE_SCALING;

        if (0==gThrottleRate)
            gThrottleRate=1;   // throttle must always have an effect

        gPerfCounters->Set(ePerfThrottleGauge, gThrottleRate);
        gPerfCounters->Add(ePerfThrottleCounter, gThrottleRate*THROTTLE_SECONDS);

        // prepare for next interval
        pthread_mutex_lock(&gThrottleMutex);
        memset(&gThrottleData[0], 0, sizeof(gThrottleData[0]));
        pthread_mutex_unlock(&gThrottleMutex);

        ++replace_idx;
        if (THROTTLE_INTERVALS==replace_idx)
            replace_idx=2;

        //
        // This is code to manage / flush the flexcache's old file cache entries.
        //  Sure there should be a better place for this code, but fits here nicely today.
        //
        if (cache_expire < now_seconds)
        {
            cache_expire = now_seconds + 60*60;  // hard coded to one hour for now
            DBList()->ScanDBs(true,&DBImpl::PurgeExpiredFileCache);
            DBList()->ScanDBs(false, &DBImpl::PurgeExpiredFileCache);
        }   // if
    }   // while

    return(NULL);

}   // ThrottleThread


void SetThrottleWriteRate(uint64_t Micros, uint64_t Keys, bool IsLevel0, int Backlog)
{
    if (IsLevel0)
    {
        pthread_mutex_lock(&gThrottleMutex);
        gThrottleData[0].m_Micros+=Micros;
        gThrottleData[0].m_Keys+=Keys;
        gThrottleData[0].m_Backlog+=Backlog;
        gThrottleData[0].m_Compactions+=1;
        pthread_mutex_unlock(&gThrottleMutex);

        gPerfCounters->Add(ePerfThrottleMicros0, Micros);
        gPerfCounters->Add(ePerfThrottleKeys0, Keys);
        gPerfCounters->Add(ePerfThrottleBacklog0, Backlog);
        gPerfCounters->Inc(ePerfThrottleCompacts0);
    }   // if

    else
    {
        pthread_mutex_lock(&gThrottleMutex);
        gThrottleData[1].m_Micros+=Micros;
        gThrottleData[1].m_Keys+=Keys;
        gThrottleData[1].m_Backlog+=Backlog;
        gThrottleData[1].m_Compactions+=1;
        pthread_mutex_unlock(&gThrottleMutex);

        gPerfCounters->Add(ePerfThrottleMicros1, Micros);
        gPerfCounters->Add(ePerfThrottleKeys1, Keys);
        gPerfCounters->Add(ePerfThrottleBacklog1, Backlog);
        gPerfCounters->Inc(ePerfThrottleCompacts1);
    }   // else

    return;
};

uint64_t GetThrottleWriteRate() {return(gThrottleRate);};

void ThrottleShutdown()
{
    if (gThrottleRunning)
    {
        gThrottleRunning=false;
        pthread_mutex_lock(&gThrottleMutex);
        pthread_cond_signal(&gThrottleCond);
        pthread_mutex_unlock(&gThrottleMutex);
        pthread_join(gThrottleThreadId, NULL);
    }   // if
}   // ThrottleShutdown

}  // namespace leveldb
