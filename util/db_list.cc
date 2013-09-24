// -------------------------------------------------------------------
//
// db_list.cc
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

#include <algorithm>

#include "util/db_list.h"
#include "util/mutexlock.h"

namespace leveldb {

// using singleton model from comparator.cc
static port::OnceType once = LEVELDB_ONCE_INIT;
static DBListImpl * dblist=NULL;

static void InitModule()
{
    dblist=new DBListImpl;
}   // InitModule


DBListImpl * DBList()
{
    port::InitOnce(&once, InitModule);
    return(dblist);

}   // DBList


void
DBListShutdown()
{
    // retrieve point to handle any initialization/shutdown races
    DBList();
    delete dblist;

    return;

}   // DBListShutdown



DBListImpl::DBListImpl()
{
}   // DBListImpl::DBListImpl


bool
DBListImpl::AddDB(
    DBImpl * Dbase,
    bool IsInternal)
{
    bool ret_flag;

    SpinLock lock(&m_Lock);

    if (IsInternal)
    {
        ret_flag=m_InternalDBs.insert(Dbase).second;
    }   // if
    else
    {
        ret_flag=m_UserDBs.insert(Dbase).second;
    }   // else

    return(ret_flag);

}   // DBListImpl::AddDB


void
DBListImpl::ReleaseDB(
    DBImpl * Dbase,
    bool IsInternal)
{
    db_set_t::iterator it;
    SpinLock lock(&m_Lock);

    if (IsInternal)
    {
        it=m_InternalDBs.find(Dbase);
        if (m_InternalDBs.end()!=it)
        {
            m_InternalDBs.erase(it);
        }   // if
    }   // if
    else
    {
        it=m_UserDBs.find(Dbase);
        if (m_UserDBs.end()!=it)
        {
            m_UserDBs.erase(it);
        }   // if
    }   // else

    return;

}   // DBListImpl::ReleaseDB


size_t
DBListImpl::GetDBCount(
    bool IsInternal)
{
    SpinLock lock(&m_Lock);

    size_t ret_val;

    if (IsInternal)
        ret_val=m_InternalDBs.size();
    else
        ret_val=m_UserDBs.size();

    return(ret_val);

}   // DBListImpl::GetDBCount


void
DBListImpl::ScanDBs(
    bool IsInternal,
    void (DBImpl::* Function)())
{
    db_set_t::iterator it, first, last;
    SpinLock lock(&m_Lock);

    // for_each() would have been fun, but setup deadlock
    //  scenarios
    // Now we have a race condition of us using the db object
    //  while someone is shutting it down ... hmm
    if (IsInternal)
    {
        first=m_InternalDBs.begin();
        last=m_InternalDBs.end();
    }   // if
    else
    {
        first=m_UserDBs.begin();
        last=m_UserDBs.end();
    }   // else

    // call member function of each database
    for (it=first; last!=it; ++it)
    {
        m_Lock.Unlock();
        ((*it)->*Function)();
        m_Lock.Lock();
    }   // for

    return;

}   // DBListImpl::ScanDBs

}  // namespace leveldb
