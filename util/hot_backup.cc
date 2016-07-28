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
#include "db/filename.h"
#include "util/db_list.h"
#include "util/hot_backup.h"
#include "util/hot_threads.h"

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
            // two instance counts held for "trigger management"
            //  (protects against hot backups happening as fast as
            //   ScanDB() makes calls ... causing trigger to reset too
            //   early)
            HotBackupScheduled();
            HotBackupScheduled();

            // Log() routes to syslog as an error
            Log(NULL, "leveldb HotBackup triggered.");
            gPerfCounters->Inc(ePerfBackupStarted);

            DBList()->ScanDBs(true,  &DBImpl::HotBackup);
            DBList()->ScanDBs(false, &DBImpl::HotBackup);

            // reduce to one instance count now that events posted
            //  (trigger reset now enabled)
            HotBackupFinished();
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
    ret_val=access(config::kTriggerFileName, F_OK);
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

    ret_val=unlink(config::kTriggerFileName);
    if (0==ret_val)
    {
        // release last counter, enabling future backups
        HotBackupFinished();
    }   // if
    else
    {
        // one count left, effectively disabling trigger forever (or next program restart)
        Log(NULL, "leveldb HotBackup unable to delete trigger file %s", config::kTriggerFileName);
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
    bool create_backup_event(false);

    // hold mutex while reviewing database state
    //  (note:  never good to call Log() while holding mutex
    //   but these are unlikely error cases)
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
                create_backup_event=true;
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
    }   // mutex released

    // post event and log message outside mutex
    if (create_backup_event)
    {
        // increment pending backup count
        HotBackupScheduled();

        // schedule backup job within compaction queue
        ThreadTask * task=new HotBackupTask(this, this->options_);
        gCompactionThreads->Submit(task, true);
        Log(options_.info_log, "HotBackup queued");
    }   // if

    return;

}   // DBImpl::HotBackup


void
HotBackupTask::operator()()
{
    bool good;

    /**** make each a function of HotBackupTask to facilitate unit test creation ****/
    // rotate directories (tiered storage reminder)
    good=PrepareDirectories();

    // grab mutex here or where?
    // ??? ++running_compactions_;  with mutex held

    // purge imm or current write buffer, get sequence number
    //  compact memtable tests if mutex_ held


    // get LOG file size
    // release mutex here?
    // snapshot
    // copy LOG to size
    // create Manifest and CURRENT in backup dir
    // create links to every manifest file


    HotBackupFinished();

    return;

}   // HotBackupTask::operator()


bool
HotBackupTask::PrepareDirectories()
{
    Options local_options;
    bool good;
    Status status;

    assert(0 < config::kNumBackups);
    good=true;

    // Destroy highest possible directory if it exists
    local_options=m_Options;
    good=SetBackupPaths(local_options, config::kNumBackups-1);
    status=DestroyDB("", local_options);

    // Rotate names of directories
    if (status.ok())
    {
        int loop;

        for (loop=config::kNumBackups-1; 1 <= loop && status.ok(); --loop)
        {
            int src, dest;
            std::string src_path, dest_path;

            src=loop-1;
            dest=loop;

            // rename fast / default tier
            src_path=BackupPath(m_Options.tiered_fast_prefix, src);
            dest_path=BackupPath(m_Options.tiered_fast_prefix, dest);

            if (m_Options.env->FileExists(src_path))
                status=m_Options.env->RenameFile(src_path, dest_path);

            // rename slow tier
            if (status.ok())
            {
                if (0<m_Options.tiered_slow_level)
                {
                    src_path=BackupPath(m_Options.tiered_slow_prefix, src);
                    dest_path=BackupPath(m_Options.tiered_slow_prefix, dest);

                    if (m_Options.env->FileExists(src_path))
                        status=m_Options.env->RenameFile(src_path, dest_path);

                    if (!status.ok())
                    {
                        good=false;
                        Log(m_DBImpl.GetLogger(), "HotBackup failed while renaming %s slow directory",
                            src_path.c_str());
                    }   // if
                }   // if
            }
            else
            {
                good=false;
                Log(m_DBImpl.GetLogger(), "HotBackup failed while renaming %s directory",
                    src_path.c_str());
            }   // else
        }   // for

    }   // if
    else
    {
        good=false;
        Log(m_DBImpl.GetLogger(), "HotBackup failed while removing %s directory",
            local_options.tiered_fast_prefix.c_str());
    }   // else


    // renaming old stuff succeeded, create new directory tree
    if (good)
    {
        local_options=m_Options;
        good=SetBackupPaths(local_options, 0);

        status=m_Options.env->CreateDir(local_options.tiered_fast_prefix);
        if (status.ok())
        {
            if (0 < local_options.tiered_slow_level)
            {
                status=m_Options.env->CreateDir(local_options.tiered_slow_prefix);
                if (!status.ok())
                {
                    good=false;
                    Log(m_DBImpl.GetLogger(), "HotBackup failed while creating %s slow directory",
                        local_options.tiered_slow_prefix.c_str());
                }   // if
            }   // if
        }   // if
        else
        {
            good=false;
            Log(m_DBImpl.GetLogger(), "HotBackup failed while creating %s directory",
                local_options.tiered_fast_prefix.c_str());
        }   // else

        // now create all the sst_? directories
        if (good)
        {
            status=MakeLevelDirectories(m_Options.env, m_Options);
            if (!status.ok())
            {
                good=false;
                Log(m_DBImpl.GetLogger(), "HotBackup failed while creating sst_ directories");
            }   // if
        }   // if
    }   // if

    return(good);

}   // HotBackupTask::PrepareDirectories




};  // namespace leveldb
