// -------------------------------------------------------------------
//
// flexcache.cc
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
#include <sys/resource.h>

#include "util/flexcache.h"

namespace leveldb {


// global cache control
FlexCache gFlexCache;


/**
 * Initialize object
 */
FlexCache::FlexCache()
    : m_TotalMemory(0)
{
    struct rlimit limit;
    int ret_val;

    // initialize total memory available based upon system data
    ret_val=getrlimit(RLIMIT_DATA, &limit);

    if (0==ret_val && RLIM_INFINITY!=limit.rlim_max)
    {
        // 2Gig is "small ram", Riak going to be tight
        if (limit.rlim_max < 2*1024*1024*1024)
            m_TotalMemory=256*1024*1024;
        else
            m_TotalMemory=(limit.rlim_max - 1024*1024*1024) / 2;
    }   // if

    return;

}   // FlexCache::FlexCache




/**
 * Return current capacity limit for cache flavor indicated,
 *  default is zero if unknown flavor.
 */
uint64_t
FlexCache::GetDBCacheCapacity(
    bool IsInternal)   //!< value describing cache attributes of caller
{
    uint64_t ret_val;

    ret_val=IsInternal*0;  // dummy line for now


/// need current size of file and block cache from db.  How to get db?

    return(ret_val);

}   // FlexCache::GetDBCacheCapacity


/**
 * Change the memory allocated to all caches, and actively resize
 *  existing caches
 */
void
FlexCache::SetTotalMemory(
    uint64_t Total)    //!< new memory allocated to all caches
{
    // only review current allocation if new value is different 
    //  and not zero default
    if (0!=Total && Total!=m_TotalMemory)
    {
        m_TotalMemory=Total;

        // ask each cache within each db to resize
    }   // if

    return;

}   // FlexCache::SetTotalMemory

}  // namespace leveldb
