// -------------------------------------------------------------------
//
// thread_tasks.h
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

// -------------------------------------------------------------------
//  Modeled after eleveldb's workitems.h/.cc
// -------------------------------------------------------------------


#ifndef STORAGE_LEVELDB_INCLUDE_THREAD_TASKS_H_
#define STORAGE_LEVELDB_INCLUDE_THREAD_TASKS_H_

#include <stdint.h>

#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/atomics.h"

namespace leveldb {


/**
 * Virtual base class for leveldb background tasks
 */
class ThreadTask
{
public:
    uint64_t m_QueueStart;        //!< NowMicros() time placed on work queue

protected:
    volatile uint32_t m_RefCount;

 public:
    ThreadTask()
        : m_QueueStart(0), m_RefCount(0) {};

    virtual ~ThreadTask() {};

    uint32_t RefInc() {return(inc_and_fetch(&m_RefCount));};

    uint32_t RefDec()
    {
        uint32_t current_refs;

        current_refs=dec_and_fetch(&m_RefCount);
        if (0==current_refs)
            delete this;

        return(current_refs);

    }   // RefObject::RefDec

    // this is the derived object's task routine
    virtual void operator()()     = 0;

private:
    ThreadTask(const ThreadTask &);
    ThreadTask & operator=(const ThreadTask &);

};  // class ThreadTask


/**
 * Background write of imm buffer to Level-0 file
 */

class ImmWriteTask : public ThreadTask
{
protected:
    DBImpl * m_DBImpl;

public:
    explicit ImmWriteTask(DBImpl * Db)
        : m_DBImpl(Db) {};

    virtual ~ImmWriteTask() {};

    virtual void operator()() {m_DBImpl->BackgroundImmCompactCall();};

private:
    ImmWriteTask();
    ImmWriteTask(const ImmWriteTask &);
    ImmWriteTask & operator=(const ImmWriteTask &);

};  // class ImmWriteTask


/**
 * Background compaction 
 */

class CompactionTask : public ThreadTask
{
protected:
    DBImpl * m_DBImpl;
    Compaction * m_Compaction;

public:
    CompactionTask(DBImpl * Db, Compaction * Compact)
        : m_DBImpl(Db), m_Compaction(Compact) {};

    virtual ~CompactionTask() {delete m_Compaction;};

    virtual void operator()() 
    {
        m_DBImpl->BackgroundCall2(m_Compaction);
        m_Compaction=NULL;
    };

private:
    CompactionTask();
    CompactionTask(const CompactionTask &);
    CompactionTask & operator=(const CompactionTask &);

};  // class CompactionTask


/**
 * Original env_posix.cc task
 */

class LegacyTask : public ThreadTask
{
protected:
    void (*m_Function)(void*);
    void * m_Arg;

public:
    LegacyTask(void (*Function)(void*), void * Arg)
        : m_Function(Function), m_Arg(Arg) {};

    virtual ~LegacyTask() {};

    virtual void operator()() 
    {
        (*m_Function)(m_Arg);
    };

private:
    LegacyTask();
    LegacyTask(const LegacyTask &);
    LegacyTask & operator=(const LegacyTask &);

};  // class LegacyTask

} // namespace leveldb


#endif  // INCL_WORKITEMS_H
