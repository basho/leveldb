// -------------------------------------------------------------------
//
// hot_backup.h
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

#ifndef STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_
#define STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_

#include "util/thread_tasks.h"

namespace leveldb
{

namespace config {

// how many hot backup directories:  backup, backup.1, ... backup.(kNumBackups-1)
static const int kNumBackups = 5;

static const char * kTriggerFileName="/etc/basho/leveldb_hot_backup";

}   // namespace config

// Called every 60 seconds to test for external hot backup trigger
//   (initiates backup if trigger seen)
void CheckHotBackupTrigger();



/**
 * Currently this class is mostly a "namespace".  Eases redirection
 *  of logic to / from unit tests.
 */
class HotBackup
{
protected:

    // tracks how many databases are still processing a hot backup request
    volatile uint64_t m_JobsPending;

public:
    HotBackup() : m_JobsPending(0) {};

protected:
    HotBackup(HotBackup * BackupTester) : m_JobsPending(0)
    {gHotBackup=BackupTester;}

public:
    virtual ~HotBackup() {};

    // allows unit test to change path name
    virtual const char * GetTriggerPath() {return(config::kTriggerFileName);};

    static HotBackup * Ptr() {return(gHotBackup);};

    uint64_t GetJobsPending() {return(add_and_fetch(&m_JobsPending, (uint64_t)0));};

    // Called by each database that is initiating a hot backup.  Blocks
    //  future hot backups until finished.
    void HotBackupScheduled();

    // Call by each database upon hot backup completion.
    void HotBackupFinished();

    // Test for external trigger to start backup
    bool IsTriggerSet();

    // Clear external trigger once every backup completes
    //  (this is flag to external process that data is ready)
    void ResetTrigger();

    bool PrepareDirectories(const Options & LiveOptions);

protected:
    // next two routines are for unit test support
    static void SetHotBackupObject(HotBackup * HB) {gHotBackup=HB;};
    static void ResetHotBackupObject();

private:

    //Singleton HotBackup object that unit tests can override.
    static HotBackup * gHotBackup;

};  // class HotBackup



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

protected:

private:
    HotBackupTask();
    HotBackupTask(const HotBackupTask &);
    HotBackupTask & operator=(const HotBackupTask &);

};  // class HotBackupTask


} // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_HOT_BACKUP_H_
