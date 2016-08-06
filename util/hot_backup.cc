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

#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "util/db_list.h"
#include "util/hot_backup.h"
#include "util/hot_threads.h"

namespace leveldb {


// this local static is used during normal operations.  unit tests
//  create independent object.
static HotBackup LocalHotBackupObj;
HotBackup * gHotBackup(&LocalHotBackupObj);
void HotBackup::ResetHotBackupObject() {gHotBackup=&LocalHotBackupObj;};


/**
 * Called by throttle.cc's thread once a minute.  Used to
 *  test for trigger condition
 */
void
CheckHotBackupTrigger()
{
    // add_and_fetch for memory fences
    if (0 == gHotBackup->GetJobsPending())
    {
        if (gHotBackup->IsTriggerSet())
        {
            // two instance counts held for "trigger management"
            //  (protects against hot backups happening as fast as
            //   ScanDB() makes calls ... causing trigger to reset too
            //   early)
            gHotBackup->HotBackupScheduled();
            gHotBackup->HotBackupScheduled();

            // Log() routes to syslog as an error
            Log(NULL, "leveldb HotBackup triggered.");
            gPerfCounters->Inc(ePerfBackupStarted);

            DBList()->ScanDBs(true,  &DBImpl::HotBackup);
            DBList()->ScanDBs(false, &DBImpl::HotBackup);

            // reduce to one instance count now that events posted
            //  (trigger reset now enabled)
            gHotBackup->HotBackupFinished();
        }   // if
    }   // if

    return;

}   // CheckHotBackupTrigger


/**
 * Look for external trigger
 */
bool
HotBackup::IsTriggerSet()
{
    bool ret_flag;
    int ret_val;

    //
    // This is code polls for existance of /etc/riak/hot_backup
    //
    ret_val=access(GetTriggerPath(), F_OK);
    ret_flag=(-1!=ret_val);

    return(ret_flag);

}   // HotBackup::IsTriggerSet


/**
 * Let external trigger know that hot backup is complete.  Put
 *  message to syslog if unable to delete trigger file
 */
void
HotBackup::ResetTrigger()
{
    bool ret_flag;
    int ret_val;

    ret_val=unlink(GetTriggerPath());
    if (0==ret_val)
    {
        // release last counter, enabling future backups
        HotBackupFinished();
    }   // if
    else
    {
        // one count left, effectively disabling trigger forever (or next program restart)
        Log(NULL, "leveldb HotBackup unable to delete trigger file %s", GetTriggerPath());
        Log(NULL, "leveldb HotBackup now disabled until program restart");
    }   // if

    return;

}   // ResetHotBackupTrigger


/**
 * A database instance marks its intent to backup
 */
void
HotBackup::HotBackupScheduled()
{
    inc_and_fetch(&JobsPending);

    return;

}   // HotBackupScheduled


/**
 * A database instance marks that its backup completed
 */
void
HotBackup::HotBackupFinished()
{
    int ret_val;

    ret_val=dec_and_fetch(&JobsPending);

    // 1 means CheckHotBackupTrigger()'s counter is only
    //   one left
    if (1==ret_val)
    {
        // hmm, call file sync() at this point?
        Log(NULL, "leveldb HotBackup complete.");
        ResetTrigger();
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
        gHotBackup->HotBackupScheduled();

        // schedule backup job within compaction queue
        ThreadTask * task=new HotBackupTask(this);
        gCompactionThreads->Submit(task, true);
        Log(options_.info_log, "HotBackup queued");
    }   // if

    return;

}   // DBImpl::HotBackup


void
HotBackupTask::operator()()
{
    bool good;
    long log_position(0);

    /**** make each a function of HotBackupTask to facilitate unit test creation ****/
    // rotate directories (tiered storage reminder)
    good=gHotBackup->PrepareDirectories(m_DBImpl.GetOptions());

    // grab mutex here or where?
    // ??? ++running_compactions_;  with mutex held

    // purge imm or current write buffer
    good=good && m_DBImpl.PurgeWriteBuffer();

    // Gather random data items for MANIFEST and
    //  other supporting files.
    if (good)
    {
        // get LOG file size
        if (NULL!=m_DBImpl.GetLogger())
            log_position=m_DBImpl.GetLogger()->LogSize();
        else
            log_position=0;

        // retrieve current version and increment refs_ (requires mutex)
        //  (includes code that creates hard links to .sst files)
        good=m_DBImpl.WriteBackupManifest();

        // last step is to replicate as much of LOG file from time of
        //  backup (close, but not exact)
        //  Assumes that 0 for log_position implies user has a custom
        //  Logger class that this routine will fail to copy.
        if (good && 0!=log_position)
            good=m_DBImpl.CopyLOGSegment(log_position);
    }   // if

    // inform db and master object that this db is done.
    m_DBImpl.HotBackupComplete();
    gHotBackup->HotBackupFinished();

    return;

}   // HotBackupTask::operator()


/**
 * Rotate directory names in a backup, backup.1, backup.2, etc. pattern.
 *  This is similar to many /var/log files on unix systems.
 */
bool
HotBackup::PrepareDirectories(
    const Options & LiveOptions)
{
    Options local_options;
    bool good;
    Status status;

    assert(0 < config::kNumBackups);
    good=true;

    // Destroy highest possible directory if it exists
    local_options=LiveOptions;
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
            src_path=BackupPath(LiveOptions.tiered_fast_prefix, src);
            dest_path=BackupPath(LiveOptions.tiered_fast_prefix, dest);

            if (LiveOptions.env->FileExists(src_path))
                status=LiveOptions.env->RenameFile(src_path, dest_path);

            // rename slow tier
            if (status.ok())
            {
                if (0<LiveOptions.tiered_slow_level)
                {
                    src_path=BackupPath(LiveOptions.tiered_slow_prefix, src);
                    dest_path=BackupPath(LiveOptions.tiered_slow_prefix, dest);

                    if (LiveOptions.env->FileExists(src_path))
                        status=LiveOptions.env->RenameFile(src_path, dest_path);

                    if (!status.ok())
                    {
                        good=false;
                        Log(LiveOptions.info_log, "HotBackup failed while renaming %s slow directory",
                            src_path.c_str());
                    }   // if
                }   // if
            }
            else
            {
                good=false;
                Log(LiveOptions.info_log, "HotBackup failed while renaming %s directory",
                    src_path.c_str());
            }   // else
        }   // for

    }   // if
    else
    {
        good=false;
        Log(LiveOptions.info_log, "HotBackup failed while removing %s directory",
            local_options.tiered_fast_prefix.c_str());
    }   // else


    // renaming old stuff succeeded, create new directory tree
    if (good)
    {
        local_options=LiveOptions;
        good=SetBackupPaths(local_options, 0);

        status=LiveOptions.env->CreateDir(local_options.tiered_fast_prefix);
        if (status.ok())
        {
            if (0 < local_options.tiered_slow_level)
            {
                status=LiveOptions.env->CreateDir(local_options.tiered_slow_prefix);
                if (!status.ok())
                {
                    good=false;
                    Log(LiveOptions.info_log, "HotBackup failed while creating %s slow directory",
                        local_options.tiered_slow_prefix.c_str());
                }   // if
            }   // if
        }   // if
        else
        {
            good=false;
            Log(LiveOptions.info_log, "HotBackup failed while creating %s directory",
                local_options.tiered_fast_prefix.c_str());
        }   // else

        // now create all the sst_? directories
        if (good)
        {
            status=MakeLevelDirectories(LiveOptions.env, local_options);
            if (!status.ok())
            {
                good=false;
                Log(LiveOptions.info_log, "HotBackup failed while creating sst_ directories");
            }   // if
        }   // if
    }   // if

    return(good);

}   // DBImpl::PrepareDirectories


/**
 * Goal is to ensure the write buffer has not been sitting
 * around for hours or days without a backup.  Not looking
 * for perfect, up to the second backup of a constant input
 * stream.
 */
bool
DBImpl::PurgeWriteBuffer()
{
    bool good(true);
    Status s;

    // existing "test"
    s=CompactMemTableSynchronous();
    good=s.ok();

    if (!good)
        Log(GetLogger(), "HotBackup failed in PurgeWriteBuffer");

    return(good);

}   // DBImpl::PurgeWriteBuffer


/**
 * Need an independent MANIFEST and CURRENT file within
 * hot backup directory.  This creates them.
 */
bool
DBImpl::WriteBackupManifest()
{
    bool good(true);
    Version * version(NULL);
    uint64_t manifest_num;
    std::string manifest_string;
    SequenceNumber sequence;
    VersionEdit edit;
    int level;
    Options local_options;
    Status status;

    // perform all non-disk activity quickly while mutex held
    {
        MutexLock l(&mutex_);
//        InternalKeyComparator const icmp(BytewiseComparator());
        InternalKeyComparator const icmp(internal_comparator_.user_comparator());

        version=versions_->current();
        version->Ref();               // must have mutex for Ref/Unref

        // reserve a file number for manifest, get a recent
        //  sequence number for "highest" (exact not needed)
        manifest_num=versions_->NewFileNumber();
        sequence=versions_->LastSequence();

        // lines taken from repair.cc
        edit.SetComparatorName(icmp.user_comparator()->Name());
        edit.SetLogNumber(0);
        edit.SetNextFile(versions_->NewFileNumber());
        edit.SetLastSequence(sequence);
    }   // mutex

    // copy list of files from Version to VersionEdit
    for (level=0; level<config::kNumLevels; ++level)
    {
        const Version::FileMetaDataVector_t level_files(version->GetFileList(level));
        Version::FileMetaDataVector_t::const_iterator it;

        for (it=level_files.begin(); it!=level_files.end(); ++it)
        {
            edit.AddFile2(level, (*it)->number, (*it)->file_size,
                          (*it)->smallest, (*it)->largest,
                          (*it)->exp_write_low, (*it)->exp_write_high, (*it)->exp_explicit_high);
        }   // for
    }   // for

    // create Manifest and CURRENT in backup dir
    local_options=options_;
    SetBackupPaths(local_options, 0);
    manifest_string=DescriptorFileName(local_options.tiered_fast_prefix, manifest_num);

    WritableFile* file;
    status = env_->NewWritableFile(manifest_string, &file, 4096);
    if (status.ok())
    {
        log::Writer log(file);
        std::string record;
        edit.EncodeTo(&record);
        status = log.AddRecord(record);

        if (status.ok())
            status = file->Close();

        delete file;
        file = NULL;

        if (!status.ok())
        {
            env_->DeleteFile(manifest_string);
            Log(GetLogger(), "HotBackup failed writing/closing manifest (%s)",
                status.ToString().c_str());
            good=false;
        }   // if
    }   // if
    else
    {
        Log(GetLogger(), "HotBackup failed creating manifest (%s)",
            status.ToString().c_str());
        good=false;
    }   // else

    // create CURRENT file that points to new manifest
    if (good)
    {
        status = SetCurrentFile(env_, local_options.tiered_fast_prefix, manifest_num);

        if (!status.ok())
        {
            env_->DeleteFile(manifest_string);
            Log(GetLogger(), "HotBackup failed to set manifest in CURRENT (%s)",
                status.ToString().c_str());
            good=false;
        }   // if
    }   // if

    // if the manifest is good, create file links
    if (good)
        good=CreateBackupLinks(version, local_options);

    // success or failure, must release version (within mutex)
    {
        MutexLock l(&mutex_);
        version->Unref();               // must have mutex for Ref/Unref
    }   // mutex

    return(good);

}   // DBImpl::WriteBackupManifest


/**
 * Create a hard link between each live file and its backup name.
 *  This forces necessary .sst files to survive in the backup directory
 *  even if no longer needed in active directory.  Also duplicates only
 *  the name, not contents ... saving tons of space.
 */
bool
DBImpl::CreateBackupLinks(
    Version * Ver,
    Options & BackupOptions)
{
    bool good(true);
    int level, ret_val;
    std::string src_path, dst_path;

    for (level=0; level<config::kNumLevels && good; ++level)
    {
        const Version::FileMetaDataVector_t level_files(Ver->GetFileList(level));
        Version::FileMetaDataVector_t::const_iterator it;

        for (it=level_files.begin(); it!=level_files.end() && good; ++it)
        {
            src_path=TableFileName(options_, (*it)->number, level);
            dst_path=TableFileName(BackupOptions, (*it)->number, level);

            // this would be cleaner if link operation added to Env class heirarchy
            ret_val=link(src_path.c_str(), dst_path.c_str());
            if (0!=ret_val)
            {
                good=false;
                Log(GetLogger(), "HotBackup failed linking (return val %d) %s",
                    ret_val, src_path.c_str());
            }   // if
        }   // for
    }   // for

    return(good);

}   // DBImpl::CreateBackupLinks


/**
 * Copy portion of LOG file that most likely applies to this backup.
 */
bool
DBImpl::CopyLOGSegment(long EndPos)
{
    assert(0<EndPos);

    bool good(true);
    long remaining;
    Options local_options;
    SequentialFile * src(NULL);
    WritableFile * dst(NULL);
    Status s;
    std::string src_name, dst_name, buffer;
    Slice data_read;

    remaining=EndPos;

    // open source and destination LOG files
    local_options=options_;
    SetBackupPaths(local_options, 0);

    src_name=InfoLogFileName(options_.tiered_fast_prefix);
    dst_name=InfoLogFileName(local_options.tiered_fast_prefix);

    s=options_.env->NewSequentialFile(src_name, &src);
    if (s.ok())
    {
        s=options_.env->NewWritableFile(dst_name, &dst, 4096);

        if (!s.ok())
            Log(GetLogger(), "HotBackup failed to create destination LOG file (%s, %s)",
                s.ToString().c_str(), dst_name.c_str());
    }   // if
    else
    {
        Log(GetLogger(), "HotBackup failed to open source LOG file (%s, %s)",
            s.ToString().c_str(), src_name.c_str());
    }   // else

    // copy data between files
    buffer.reserve(4096);

    while(0<remaining && s.ok())
    {
        long count;

        count=(4096<remaining ? 4096 : remaining);
        s=src->Read(count, &data_read, (char *)buffer.data());

        if (s.ok())
        {
            s=dst->Append(data_read);

            if (!s.ok())
                Log(GetLogger(), "HotBackup LOG write failed at %ld (%s)",
                    remaining, s.ToString().c_str());
        }   // if
        else
        {
                Log(GetLogger(), "HotBackup LOG read failed at %ld (%s)",
                    remaining, s.ToString().c_str());
        }   // else

        remaining-=count;
    }   // while

    // close files and go home
    good=s.ok();
    delete src;
    delete dst;

    return(good);

}   // DBImpl::CopyLOGSegment


void
DBImpl::HotBackupComplete()
{
    MutexLock l(&mutex_);
    hotbackup_pending_=false;
    bg_cv_.SignalAll();

    return;

}   // DBImpl::HotBackupComplete
};  // namespace leveldb
