// -------------------------------------------------------------------
//
// hot_backup.cc
//
// Copyright (c) 2011-2016 Basho Technologies, Inc. All Rights Reserved.
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

#include <unistd.h>

#include "leveldb/atomics.h"
#include "leveldb/env.h"

#include "db/db_impl.h"
#include "util/db_list.h"
#include "util/hot_threads.h"
#include "util/thread_tasks.h"

namespace leveldb {


// Called by each database that is initiating a hot backup.  Blocks
//  future hot backups until finished.
void HotBackupScheduled();

// Call by each database upon hot backup completion.
void HotBackupFinished();

// Test for external trigger to start backup
static bool IsHotBackupTriggerSet();

// Clear external trigger once every backup completes
//  (this is flag to external process that data is ready)
static void ResetHotBackupTrigger();

// tracks how many databases are still processing a hot backup request
static volatile uint64_t lHotBackupJobsPending(0);

static const char * lTriggerFileName="/etc/riak/hot_backup";


/**
 * Background task to perform backup steps on compaction thread
 */
class HotBackupTask : public ThreadTask
{
protected:

    DBImpl & m_DBImpl;

public:
    HotBackupTask(DBImpl * DB_ptr)
    : m_DBImpl(*DB_ptr)
    {};

    virtual ~HotBackupTask() {};

    virtual void operator()();

private:
    HotBackupTask();
    HotBackupTask(const HotBackupTask &);
    HotBackupTask & operator=(const HotBackupTask &);

};  // class HotBackupTask


/**
 * Called by throttle.cc's thread once a minute.  Used to
 *  test for trigger condition
 */
void
CheckHotBackupTrigger()
{
    // add_and_fetch for memory fences
    if (0 == add_and_fetch(&lHotBackupJobsPending, (uint64_t)0))
    {
        if (IsHotBackupTriggerSet())
        {
            // one instance count held for "trigger management"
            HotBackupScheduled();

            // Log() routes to syslog as an error
            Log(NULL, "leveldb HotBackup triggered.");
            gPerfCounters->Inc(ePerfBackupStarted);

            DBList()->ScanDBs(true,  &DBImpl::HotBackup);
            DBList()->ScanDBs(false, &DBImpl::HotBackup);
        }   // if
    }   // if

    return;

}   // CheckHotBackupTrigger


/**
 * Look for external trigger
 */
static bool
IsHotBackupTriggerSet()
{
    bool ret_flag;
    int ret_val;

    //
    // This is code polls for existance of /etc/riak/hot_backup
    //
    ret_val=access(lTriggerFileName, F_OK);
    ret_flag=(-1!=ret_val);

    return(ret_flag);

}   // IsHotBackupTriggerSet


/**
 * Let external trigger know that hot backup is complete.  Put
 *  message to syslog if unable to delete trigger file
 */
static void
ResetHotBackupTrigger()
{
    bool ret_flag;
    int ret_val;

    ret_val=unlink(lTriggerFileName);
    if (0==ret_val)
    {
        // release last counter, enabling future backups
        HotBackupFinished();
    }   // if
    else
    {
        // one count left, effectively disabling trigger forever (or next program restart)
        Log(NULL, "leveldb HotBackup unable to delete trigger file %s", lTriggerFileName);
        Log(NULL, "leveldb HotBackup now disabled until program restart");
    }   // if

    return;

}   // ResetHotBackupTrigger


/**
 * A database instance marks its intent to backup
 */
void
HotBackupScheduled()
{
    inc_and_fetch(&lHotBackupJobsPending);

    return;

}   // HotBackupScheduled


/**
 * A database instance marks that its backup completed
 */
void
HotBackupFinished()
{
    int ret_val;

    ret_val=dec_and_fetch(&lHotBackupJobsPending);

    // 1 means CheckHotBackupTrigger()'s counter is only
    //   one left
    if (1==ret_val)
    {
        // hmm, call file sync() at this point?
        Log(NULL, "leveldb HotBackup complete.");
        ResetHotBackupTrigger();
    }   // if

    return;

}   // HotBackupFinished


/**
 * Routine called by DBList()->ScanDBs.  This has full context of one open
 *  leveldb database.  Validates then schedules a backup job via compaction
 *  threads.
 */
void
DBImpl::HotBackup()
{
    // get mutex,
    MutexLock l(&mutex_);

    // set backup flag && see if
    assert(false==hotbackup_pending_);
    if (!hotbackup_pending_)
    {
        hotbackup_pending_=true;

        // test if shutting_down,
        if (!shutting_down_.Acquire_Load())
        {
            // increment pending backup count
            HotBackupScheduled();

            // schedule backup job within compaction queue
            ThreadTask * task=new HotBackupTask(this);
            gCompactionThreads->Submit(task, true);
            Log(options_.info_log, "HotBackup queued");
        }   // if
        else
        {
            Log(options_.info_log, "HotBackup aborted, shutdown in progress");
        }   // else
    }   // if
    else
    {
        Log(options_.info_log, "HotBackup aborted, backup already in progress");
    }   // else

    return;

}   // DBImpl::HotBackup


void
HotBackupTask::operator()()
{
    /**** make each a function of HotBackupTask to facilitate unit test creation ****/
    // rotate directories (tiered storage reminder)
    // create new backup directory (tiered storage reminder)
    // grab mutex here or where?
    // purge imm or current write buffer, get sequence number
    // get LOG file size
    // release mutex here?
    // snapshot
    // copy LOG to size
    // create Manifest and CURRENT in backup dir
    // create links to every manifest file


    HotBackupFinished();

    return;

}   // HotBackupTask::operator()


};  // namespace leveldb
