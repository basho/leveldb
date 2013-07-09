// -------------------------------------------------------------------
//
// throttle.h
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

#include <pthread.h>


namespace leveldb {

extern pthread_rwlock_t gThreadLock0;
extern pthread_rwlock_t gThreadLock1;

extern pthread_mutex_t gThrottleMutex;

#define THROTTLE_INTERVALS 63

struct ThrottleData_t
{
    uint64_t m_Micros;
    uint64_t m_Keys;
    uint64_t m_Backlog;
    uint64_t m_Compactions;
};

extern ThrottleData_t gThrottleData[THROTTLE_INTERVALS];
extern uint64_t gThrottleRate;


void ThrottleInit(Env * env);


}  // namespace leveldb
