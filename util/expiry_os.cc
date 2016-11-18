// -------------------------------------------------------------------
//
// expiry_os.cc
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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <limits.h>

#include "leveldb/perf_count.h"
#include "leveldb/env.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "util/expiry_os.h"
#include "util/logging.h"
#include "util/throttle.h"

namespace leveldb {

// sext key for Riak's meta data
static const char * lRiakMetaDataKey=
    {"\x10\x00\x00\x00\x02\x0c\xb6\xd9\x00\x08"};
static const size_t lRiakMetaDataKeyLen=10;

/**
 * settings information that gets dumped to LOG upon
 *  leveldb start
 */
void
ExpiryModuleOS::Dump(
    Logger * log) const
{
    Log(log," ExpiryModuleOS.expiry_enabled: %s", expiry_enabled ? "true" : "false");
    Log(log," ExpiryModuleOS.expiry_minutes: %" PRIu64, expiry_minutes);
    Log(log,"    ExpiryModuleOS.whole_files: %s", whole_file_expiry ? "true" : "false");

    return;

}   // ExpiryModuleOS::Dump


bool
ExpiryModuleOS::MemTableInserterCallback(
    const Slice & Key,   // input: user's key about to be written
    const Slice & Value, // input: user's value object
    ValueType & ValType,   // input/output: key type. call might change
    ExpiryTime & Expiry)   // input/output: 0 or specific expiry. call might change
    const
{
    bool good(true);

    // only update the expiry time if explicit type
    //  without expiry, OR ExpiryMinutes set and not internal key
    if ((kTypeValueWriteTime==ValType && 0==Expiry)
        || (kTypeValue==ValType
            && 0!=expiry_minutes
            && expiry_enabled
            && (Key.size()<lRiakMetaDataKeyLen
                || 0!=memcmp(lRiakMetaDataKey,Key.data(),lRiakMetaDataKeyLen))))
    {
        ValType=kTypeValueWriteTime;
        Expiry=GenerateWriteTime(Key, Value);
    }   // if

    return(good);

}   // ExpiryModuleOS::MemTableInserterCallback


/**
 * Use Basho's GetTimeMinutes() as write time source.
 *  This clock returns microseconds since epoch, but
 *  only updates every 60 seconds or so.
 */
uint64_t
ExpiryModuleOS::GenerateWriteTime(
    const Slice & Key,
    const Slice & Value) const
{

    return(GetTimeMinutes());

}  // ExpiryModuleOS::GenerateWriteTime()

/**
 * Returns true if key is expired.  False if key is NOT expired
 *  (used by MemtableCallback() too)
 */
bool ExpiryModuleOS::KeyRetirementCallback(
    const ParsedInternalKey & Ikey) const
{
    bool is_expired(false);
    uint64_t now, expires;

    if (expiry_enabled)
    {
        switch(Ikey.type)
        {
            case kTypeDeletion:
            case kTypeValue:
            default:
                is_expired=false;
                break;

            case kTypeValueWriteTime:
                if (0!=expiry_minutes && 0!=Ikey.expiry)
                {
                    now=GetTimeMinutes();
                    expires=expiry_minutes*60*port::UINT64_ONE_SECOND + Ikey.expiry;
                    is_expired=(expires<=now);
                }   // if
                break;

            case kTypeValueExplicitExpiry:
                if (0!=Ikey.expiry)
                {
                    now=GetTimeMinutes();
                    is_expired=(Ikey.expiry<=now);
                }   // if
                break;
        }   // switch
    }   // if

    return(is_expired);

}   // ExpiryModuleOS::KeyRetirementCallback


/**
 *  - Sets low/high date range for aged expiry.
 *     (low for possible time series optimization)
 *  - Sets high date range for explicit expiry.
 *  - Increments delete counter for things already
 *     expired (to aid in starting compaction for
 *     keys tombstoning for higher levels).
 */
bool ExpiryModuleOS::TableBuilderCallback(
    const Slice & Key,
    SstCounters & Counters) const
{
    bool good(true);
    ExpiryTime expires, temp;

    if (IsExpiryKey(Key))
        expires=ExtractExpiry(Key);
    else
        expires=0;

    // make really high so that everything is less than it
    if (1==Counters.Value(eSstCountKeys))
        Counters.Set(eSstCountExpiry1, ULONG_MAX);

    // only updating counters.  do this even if
    //  expiry disabled
    switch(ExtractValueType(Key))
    {
        // exp_write_low set to smallest (earliest) write time
        // exp_write_high set to largest (most recent) write time
        case kTypeValueWriteTime:
            temp=Counters.Value(eSstCountExpiry1);
            if (expires<temp)
                Counters.Set(eSstCountExpiry1, expires);
            if (Counters.Value(eSstCountExpiry2)<expires)
                Counters.Set(eSstCountExpiry2, expires);
            // add to delete count if expired already
            //   i.e. acting as a tombstone
            if (0!=expiry_minutes && MemTableCallback(Key))
                Counters.Inc(eSstCountDeleteKey);
            break;

        case kTypeValueExplicitExpiry:
            if (Counters.Value(eSstCountExpiry3)<expires)
                Counters.Set(eSstCountExpiry3, expires);
            // add to delete count if expired already
            //   i.e. acting as a tombstone
            if (0!=expiry_minutes && MemTableCallback(Key))
                Counters.Inc(eSstCountDeleteKey);
            break;

        // at least one non-expiry, exp_write_low gets zero
        case kTypeValue:
            Counters.Set(eSstCountExpiry1, 0);
            break;

        default:
            break;
    }   // switch

    return(good);

}   // ExpiryModuleOS::TableBuilderCallback


/**
 * Returns true if key is expired.  False if key is NOT expired
 */
bool ExpiryModuleOS::MemTableCallback(
    const Slice & InternalKey) const
{
    bool is_expired(false), good;
    ParsedInternalKey parsed;

    good=ParseInternalKey(InternalKey, &parsed);

    if (good)
        is_expired=KeyRetirementCallback(parsed);

    return(is_expired);

}   // ExpiryModuleOS::MemTableCallback


/**
 * Returns true if at least one file on this level
 *  is eligible for full file expiry
 */
bool ExpiryModuleOS::CompactionFinalizeCallback(
    bool WantAll,                  // input: true - examine all expired files
    const Version & Ver,           // input: database state for examination
    int Level,                     // input: level to review for expiry
    VersionEdit * Edit) const      // output: NULL or destination of delete list
{
    bool ret_flag(false);

    if (expiry_enabled && whole_file_expiry)
    {
        bool expired_file(false);
        ExpiryTime now, aged;
        const std::vector<FileMetaData*> & files(Ver.GetFileList(Level));
        std::vector<FileMetaData*>::const_iterator it;

        now=GetTimeMinutes();
        aged=now - expiry_minutes*60*port::UINT64_ONE_SECOND;
        for (it=files.begin(); (!expired_file || WantAll) && files.end()!=it; ++it)
        {
            // First, find an eligible file:
            //  - if exp_write_low is zero, game over -  contains non-expiry records
            //  - if exp_write_high is below current aged time and aging enabled,
            //       or no exp_write_high keys (is zero)
            //  - highest explicit expiry (exp_explicit_high) is non-zero and below now
            //  Note:  say file only contained deleted records:  ... still delete file
            //      exp_write_low would be ULONG_MAX, exp_write_high would be 0, exp_explicit_high would be zero
            expired_file = (0!=(*it)->exp_write_low) && (0!=(*it)->exp_write_high || 0!=(*it)->exp_explicit_high);
            expired_file = expired_file && (((*it)->exp_write_high<=aged && 0!=expiry_minutes)
                                            || 0==(*it)->exp_write_high);

            expired_file = expired_file && (0==(*it)->exp_explicit_high
                                            || (0!=(*it)->exp_explicit_high && (*it)->exp_explicit_high<=now));

            // identified an expired file, do any higher levels overlap
            //  its key range?
            if (expired_file)
            {
                int test;
                Slice small, large;

                for (test=Level+1;
                     test<config::kNumLevels && expired_file;
                     ++test)
                {
                    small=(*it)->smallest.user_key();
                    large=(*it)->largest.user_key();
                    expired_file=!Ver.OverlapInLevel(test, &small,
                                                     &large);
                }   // for
                ret_flag=ret_flag || expired_file;
            }   // if

            // expired_file and no overlap? mark it for delete
            if (expired_file && NULL!=Edit)
            {
                Edit->DeleteFile((*it)->level, (*it)->number);
            }   // if
        }   // for
    }   // if

    return(ret_flag);

}   // ExpiryModuleOS::CompactionFinalizeCallback


/**
 * Riak specific routine to process whole file expiry.
 *  Code here derived from DBImpl::CompactMemTable() in db/db_impl.cc
 */
Status
DBImpl::BackgroundExpiry(
    Compaction * Compact)
{
    Status s;
    size_t count;

    mutex_.AssertHeld();
    assert(NULL != Compact && NULL!=options_.expiry_module.get());
    assert(NULL != Compact->version());

    if (NULL!=Compact && options_.ExpiryActivated())
    {
        VersionEdit edit;
        int level(Compact->level());

        // Compact holds a reference count to version()/input_version_
        const Version* base = Compact->version();
        options_.expiry_module->CompactionFinalizeCallback(true, *base, level,
                                                           &edit);
        count=edit.DeletedFileCount();

        if (s.ok() && shutting_down_.Acquire_Load()) {
            s = Status::IOError("Deleting DB during expiry compaction");
        }

        // push expired list to manifest
        if (s.ok() && 0!=count)
        {
            s = versions_->LogAndApply(&edit, &mutex_);
            if (s.ok())
                gPerfCounters->Add(ePerfExpiredFiles, count);
            else
                s = Status::IOError("LogAndApply error during expiry compaction");
        }   // if

        // Commit to the new state
        if (s.ok() && 0!=count)
        {
            // get rid of Compact now to potential free
            //  input version's files
            delete Compact;
            Compact=NULL;

            DeleteObsoleteFiles();

            // release mutex when writing to log file
            mutex_.Unlock();

            Log(options_.info_log,
                "Expired: %zd files from level %d",
                count, level);
            mutex_.Lock();
        }   // if
    }   // if

    // convention in BackgroundCompaction() is to delete Compact here
    delete Compact;

    return s;

}   // DBImpl:BackgroundExpiry


}  // namespace leveldb
