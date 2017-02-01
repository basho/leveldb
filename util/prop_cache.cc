// -------------------------------------------------------------------
//
// prop_cache.cc
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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
#include <unistd.h>

#include "util/prop_cache.h"
//#include "leveldb_ee/riak_object.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/throttle.h"

namespace leveldb {

// initialize the static variable in hopes we do not later
//  attempt a shutdown against an uninitialized value (could still happen)
static PropertyCache * lPropCache(NULL);

/**
 * Create the cache.  Called only once upon
 * leveldb initialization
 */
void
PropertyCache::InitPropertyCache(
    EleveldbRouter_t Router)
{
    ShutdownPropertyCache();
    lPropCache = new PropertyCache(Router);

    return;

}   // PropertyCache


void
PropertyCache::ShutdownPropertyCache()
{
    delete lPropCache;
    lPropCache=NULL;

}  // PropertyCache::ShutdownPropertyCache


/**
 * Unit test support.  Allows use of derived versions
 *  of PropertyCache that easy testing
 */
void
PropertyCache::SetGlobalPropertyCache(
    PropertyCache * NewGlobal)
{
    // (creates infinite loop) ShutdownPropertyCache();
    lPropCache = NewGlobal;

    return;

}   // PropertyCache::SetGlobalPropertyCache


Cache &
PropertyCache::GetCache()
{

    return(*lPropCache->GetCachePtr());

}   // PropertyCache::GetCache


PropertyCache *
PropertyCache::GetPropertyCachePtr()
{
    return(lPropCache);
}   // PropertyCache::GetPropertyCachePtr


/**
 * Construct property cache object (likely singleton)
 */
PropertyCache::PropertyCache(
    EleveldbRouter_t Router)
    : m_Cache(NULL), m_Router(Router),
      m_Cond(&m_Mutex)
{
    m_Cache = NewLRUCache2(GetCacheLimit());

}   // PopertyCache::PropertyCache


PropertyCache::~PropertyCache()
{
    delete m_Cache;
    m_Cache=NULL;
}   // PropertyCache::~PropertyCache


/**
 * Retrieve property from cache if available,
 *  else call out to Riak to get properties
 */
Cache::Handle *
PropertyCache::Lookup(
    const Slice & CompositeBucket)
{
    Cache::Handle * ret_handle(NULL);

    if (NULL!=lPropCache)
    {
        ret_handle=lPropCache->LookupInternal(CompositeBucket);
    }   // if

    return(ret_handle);

}   // PropertyCache::Lookup


/**
 * Retrieve property from cache if available,
 *  else call out to Riak to get properties
 */
Cache::Handle *
PropertyCache::LookupInternal(
    const Slice & CompositeBucket)
{
    Cache::Handle * ret_handle(NULL);

    if (NULL!=m_Cache)
    {
        ret_handle=m_Cache->Lookup(CompositeBucket);

        // force a reread of properties every 5 minutes
        if (NULL!=ret_handle)
        {
            uint64_t now;
            ExpiryModule * mod_ptr;

            now=GetTimeMinutes();
            mod_ptr=(ExpiryModule *)m_Cache->Value(ret_handle);

            // some unit tests of mod_ptr of NULL
            if (NULL!=mod_ptr && 0!=mod_ptr->ExpiryModuleExpiry()
                && mod_ptr->ExpiryModuleExpiry()<now)
            {
                m_Cache->Release(ret_handle);
                m_Cache->Erase(CompositeBucket);
                ret_handle=NULL;
            }   // if
        }   // if

        // not waiting in the cache already.  Request info
        if (NULL==ret_handle && NULL!=m_Router)
        {
            // call to Riak required
            ret_handle=LookupWait(CompositeBucket);
            gPerfCounters->Inc(ePerfPropCacheMiss);
        }   // if
        else if (NULL!=ret_handle)
        {
            // cached or no router
            gPerfCounters->Inc(ePerfPropCacheHit);
        }   // else if
    }   // if

    // never supposed to be missing if property cache in play
    if (NULL==ret_handle)
        gPerfCounters->Inc(ePerfPropCacheError);

    return(ret_handle);

}   // PropertyCache::LookupInternal


/**
 * Callback function used when Cache drops an object
 *  to make room for another due to cache size being exceeded
 */
static void
DeleteProperty(
    const Slice& key,
    void* value)
{
    ExpiryModuleOS * expiry;

    expiry=(ExpiryModuleOS *)value;

    delete expiry;
}   // static DeleteProperty


/**
 * (static) Add / Overwrite key in property cache.  Manage handle
 *  on caller's behalf
 */
bool
PropertyCache::Insert(
    const Slice & CompositeBucket,
    void * Props,
    Cache::Handle ** OutputPtr)
{
    bool ret_flag(false);
    Cache::Handle * ret_handle(NULL);

    if (NULL!=lPropCache && NULL!=lPropCache->GetCachePtr())
    {
        ret_handle=lPropCache->InsertInternal(CompositeBucket, Props);

        if (NULL!=OutputPtr)
            *OutputPtr=ret_handle;
        else if (NULL!=ret_handle)
            GetCache().Release(ret_handle);

        ret_flag=(NULL!=ret_handle);
    }   // if

    return(ret_flag);

}   // PropertyCache::Insert


Cache::Handle *
PropertyCache::InsertInternal(
    const Slice & CompositeBucket,
    void * Props)
{
    assert(NULL!=m_Cache);

    Cache::Handle * ret_handle(NULL);

    {
        MutexLock lock(&m_Mutex);

        ret_handle=m_Cache->Insert(CompositeBucket, Props, 1, DeleteProperty);
        m_Cond.SignalAll();
    }

    return(ret_handle);

}   // PropertyCache::InsertInternal

}  // namespace leveldb
