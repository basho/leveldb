// -------------------------------------------------------------------
//
// expiry_ee_tests.cc
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

#include <limits.h>
#include <algorithm>

#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#include "leveldb_ee/expiry_ee.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/version_set.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "util/throttle.h"

/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {

// helper function to clean up heap objects
static void ClearMetaArray(Version::FileMetaDataVector_t & ClearMe);


/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class ExpiryTester
{
public:
    ExpiryTester()
    {
    };

    ~ExpiryTester()
    {
    };
};  // class ExpiryTester


/**
 * Validate option defaults
 */
TEST(ExpiryTester, Defaults)
{
    ExpiryModuleEE expiry;

    ASSERT_EQ(expiry.expiry_enabled, false);
    ASSERT_EQ(expiry.expiry_minutes, 0);
    ASSERT_EQ(expiry.whole_file_expiry, false);

}   // test Defaults


/**
 * Validate MemTableInserterCallback
 */
TEST(ExpiryTester, MemTableInserterCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    ValueType type;
    ExpiryTime expiry;
    Slice key, value;

    module.expiry_enabled=true;
    module.whole_file_expiry=true;

    // deletion, do nothing
    type=kTypeDeletion;
    expiry=0;
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeDeletion);
    ASSERT_EQ(expiry, 0);

    // plain value, needs expiry
    type=kTypeValue;
    expiry=0;
    module.expiry_minutes=30;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);

    // plain value, expiry disabled
    type=kTypeValue;
    expiry=0;
    module.expiry_minutes=0;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValue);
    ASSERT_EQ(expiry, 0);

    // write time value, needs expiry
    type=kTypeValueWriteTime;
    expiry=0;
    module.expiry_minutes=30;
    before=port::TimeUint64();
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE(before <= expiry && expiry <=after && 0!=expiry);

    // write time value, expiry supplied (as if copied from another db)
    type=kTypeValueWriteTime;
    module.expiry_minutes=30;
    before=port::TimeUint64();
    expiry=before - 1000;
    SetTimeMinutes(before);
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    after=port::TimeUint64();
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueWriteTime);
    ASSERT_TRUE((before - 1000) == expiry && expiry <=after && 0!=expiry);

    // explicit expiry, not changed
    type=kTypeValueExplicitExpiry;
    expiry=97531;
    module.expiry_minutes=30;
    flag=module.MemTableInserterCallback(key, value, type, expiry);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(type, kTypeValueExplicitExpiry);
    ASSERT_EQ(expiry, 97531);

}   // test MemTableInserterCallback


/**
 * Validate MemTableCallback
 *   (supports KeyRetirementCallback in generic case)
 */
TEST(ExpiryTester, MemTableCallback)
{
    bool flag;
    uint64_t before, after;
    ExpiryModuleEE module;
    ValueType type;
    ExpiryTime expiry;
    Slice key, value;

    module.expiry_enabled=true;
    module.whole_file_expiry=true;
    module.expiry_minutes=5;

    before=port::TimeUint64();
    SetTimeMinutes(before);

    // deletion, do nothing
    InternalKey key1("DeleteMeKey", 0, 0, kTypeDeletion);
    flag=module.MemTableCallback(key1.internal_key());
    ASSERT_EQ(flag, false);

    // plain value, no expiry
    InternalKey key2("PlainKey", 0, 0, kTypeValue);
    flag=module.MemTableCallback(key2.internal_key());
    ASSERT_EQ(flag, false);

    // explicit, but time in the future
    after=GetTimeMinutes() + 60*port::UINT64_ONE_SECOND;
    InternalKey key3("ExplicitKey", after, 0, kTypeValueExplicitExpiry);
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, false);
    // advance the clock
    SetTimeMinutes(after + 60*port::UINT64_ONE_SECOND);
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, true);
    // disable expiry
    module.expiry_enabled=false;
    flag=module.MemTableCallback(key3.internal_key());
    ASSERT_EQ(flag, false);

    // age expiry
    module.expiry_enabled=true;
    module.expiry_minutes=2;
    after=GetTimeMinutes();
    InternalKey key4("AgeKey", after, 0, kTypeValueWriteTime);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);
    // advance the clock
    SetTimeMinutes(after + 60*port::UINT64_ONE_SECOND);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);
    SetTimeMinutes(after + 120*port::UINT64_ONE_SECOND);
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, true);
    // disable expiry
    module.expiry_enabled=false;
    flag=module.MemTableCallback(key4.internal_key());
    ASSERT_EQ(flag, false);

}   // test MemTableCallback


/**
 * Wrapper class to Version that allows manipulation
 *  of internal objects for testing purposes
 */
class VersionTester : public Version
{
public:
    VersionTester() : Version(&m_Vset), m_Icmp(m_Options.comparator),
                      m_Vset("", &m_Options, NULL, &m_Icmp)  {};

    void SetFileList(int Level, FileMetaDataVector_t & Files)
        {files_[Level]=Files;};

    Options m_Options;
    InternalKeyComparator m_Icmp;
    VersionSet m_Vset;
};  // class VersionTester


/**
 * Validate CompactionFinalizeCallback's
 *  identification of expired files
 */

TEST(ExpiryTester, CompactionFinalizeCallback1)
{
    bool flag;
    uint64_t now, aged, temp_time;
    std::vector<FileMetaData*> files;
    FileMetaData * file_ptr;
    ExpiryModuleEE module;
    VersionTester ver;
    int level;

    module.expiry_enabled=true;
    module.whole_file_expiry=true;
    module.expiry_minutes=5;
    level=config::kNumOverlapLevels;

    now=port::TimeUint64();
    SetTimeMinutes(now);

    // put two files into the level, no expiry
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("AA1", 0, 1, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("CC1", 0, 2, kTypeValue));
    files.push_back(file_ptr);

    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("DD1", 0, 3, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("FF1", 0, 4, kTypeValue));
    files.push_back(file_ptr);

    // disable
    module.expiry_enabled=false;
    module.whole_file_expiry=false;
    module.expiry_minutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable and move clock
    module.expiry_enabled=true;
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    SetTimeMinutes(now + 120*port::UINT64_ONE_SECOND);
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // add file only containing explicit
    //  (explicit only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("GG1", 0, 5, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("HH1", 0, 6, kTypeValue));
    file_ptr->expiry1=ULONG_MAX;  // sign of no aged expiry, or plain keys
    file_ptr->expiry3=now + 60*port::UINT64_ONE_SECOND;
    files.push_back(file_ptr);

    // disable
    module.expiry_enabled=false;
    module.whole_file_expiry=false;
    module.expiry_minutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable compaction expiry only
    module.expiry_enabled=true;
    module.whole_file_expiry=false;
    module.expiry_minutes=1;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable file expiry too
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    //   ... but file without aged expiries or plain keys
    module.whole_file_expiry=true;
    module.expiry_minutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // remove explicit
    files.pop_back();
    delete file_ptr;

    // add file only containing aged
    //  (aging only shown in counts, not keys)
    file_ptr=new FileMetaData;
    file_ptr->smallest.SetFrom(ParsedInternalKey("II1", 0, 7, kTypeValue));
    file_ptr->largest.SetFrom(ParsedInternalKey("JJ1", 0, 8, kTypeValue));
    file_ptr->expiry1=now - 60*port::UINT64_ONE_SECOND;
    file_ptr->expiry2=now + 60*port::UINT64_ONE_SECOND;
    files.push_back(file_ptr);

    // disable
    module.whole_file_expiry=false;
    module.expiry_minutes=0;
    ver.SetFileList(level, files);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable compaction only
    module.whole_file_expiry=false;
    module.expiry_minutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // enable file too
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // enable file, but not expiry minutes (disable)
    module.whole_file_expiry=true;
    module.expiry_minutes=0;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 5 min
    module.whole_file_expiry=true;
    module.expiry_minutes=5;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 30 seconds
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    SetTimeMinutes(now + 30*port::UINT64_ONE_SECOND);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 1.5minutes
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    SetTimeMinutes(now + 90*port::UINT64_ONE_SECOND);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // file_ptr at 1min, setting at 1m, clock at 2minutes
    module.whole_file_expiry=true;
    module.expiry_minutes=1;
    SetTimeMinutes(now + 120*port::UINT64_ONE_SECOND);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // same settings, but show an explicit expiry too that has not
    //  expired
    file_ptr->expiry3=now +240*port::UINT64_ONE_SECOND;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // same settings, but show an explicit expiry has expired
    //  expired
    file_ptr->expiry3=now +90*port::UINT64_ONE_SECOND;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // bug 1 - thank you Paul Place
    // try having the expired file first in the list, followed by non-expired files
    std::vector<FileMetaData*> files1(files.size());
    std::reverse_copy(files.begin(), files.end(), files1.begin());
    ver.SetFileList(level, files1);
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);
    ver.SetFileList(level, files);

    // same settings, explicit has expired, but not the aged
    //  expired
    file_ptr->expiry2=now +240*port::UINT64_ONE_SECOND;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, false);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, false);

    // variations on Bug 1 test.  Put singleton expired file in
    //  first, second, then third position.  Other two no expiry
    files[0]->expiry1=ULONG_MAX;  // sign of no aged expiry, or plain keys
    files[0]->expiry2=0;
    files[0]->expiry3=now +90*port::UINT64_ONE_SECOND;
    files[1]->expiry1=ULONG_MAX;  // sign of no aged expiry, or plain keys
    files[1]->expiry2=0;
    files[1]->expiry3=0;
    files[2]->expiry1=ULONG_MAX;  // sign of no aged expiry, or plain keys
    files[2]->expiry2=0;
    files[2]->expiry3=0;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);
    files[0]->expiry3=0;
    files[1]->expiry3=now +90*port::UINT64_ONE_SECOND;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);
    files[1]->expiry3=0;
    files[2]->expiry3=now +90*port::UINT64_ONE_SECOND;
    flag=module.CompactionFinalizeCallback(true, ver, level, NULL);
    ASSERT_EQ(flag, true);
    flag=module.CompactionFinalizeCallback(false, ver, level, NULL);
    ASSERT_EQ(flag, true);

    // clean up phony files or Version destructor will crash
    ClearMetaArray(files);
    ver.SetFileList(level,files);

}   // test CompactionFinalizeCallback


/**
 * Building static sets of file levels to increase visibility
 */

struct TestFileMetaData
{
    uint64_t m_Number;          // file number
    const char * m_Smallest;
    const char * m_Largest;
    ExpiryTime m_Expiry1;              // minutes
    ExpiryTime m_Expiry2;
    ExpiryTime m_Expiry3;
};


static void
ClearMetaArray(
    Version::FileMetaDataVector_t & ClearMe)
{
    // clean up phony files or Version destructor will crash
    std::vector<FileMetaData*>::iterator it;
    for (it=ClearMe.begin(); ClearMe.end()!=it; ++it)
        delete (*it);
    ClearMe.clear();

}   // ClearMetaArray


static void
CreateMetaArray(
    Version::FileMetaDataVector_t & Output,
    TestFileMetaData * Data,
    size_t Count)
{
    size_t loop;
    TestFileMetaData * cursor;
    FileMetaData * file_ptr;
    ExpiryTime now;

    ClearMetaArray(Output);
    now=GetTimeMinutes();

    for (loop=0, cursor=Data; loop<Count; ++loop, ++cursor)
    {
        file_ptr=new FileMetaData;
        file_ptr->number=cursor->m_Number;
        file_ptr->smallest.SetFrom(ParsedInternalKey(cursor->m_Smallest, 0, cursor->m_Number, kTypeValue));
        file_ptr->largest.SetFrom(ParsedInternalKey(cursor->m_Largest, 0, cursor->m_Number, kTypeValue));
        if (0!=cursor->m_Expiry1)
        {
            if (ULONG_MAX!=cursor->m_Expiry1)
                file_ptr->expiry1=now + cursor->m_Expiry1*60000000;
            else
                file_ptr->expiry1=cursor->m_Expiry1;
        }   // if

        if (0!=cursor->m_Expiry2)
            file_ptr->expiry2=now + cursor->m_Expiry2*60000000;

        if (0!=cursor->m_Expiry3)
            file_ptr->expiry3=now + cursor->m_Expiry3*60000000;

        Output.push_back(file_ptr);
    }   // for

}   // CreateMetaArray


/** case: two levels, no overlap, no expiry **/
TestFileMetaData levelA[]=
{
    {100, "AA", "BA", 0, 0, 0},
    {101, "LA", "NA", 0, 0, 0}
};  // levelA

TestFileMetaData levelB[]=
{
    {200, "CA", "DA", 0, 0, 0},
    {201, "SA", "TA", 0, 0, 0}
};  // levelB


/** case: two levels, 100% overlap, both levels expired **/
TestFileMetaData levelC[]=
{
    {200, "CA", "DA", 1, 3, 0},
    {201, "SA", "TA", ULONG_MAX, 0, 4}
};  // levelC

TestFileMetaData levelD[]=
{
    {200, "CA", "DA", 1, 2, 0},
    {201, "SA", "TA", ULONG_MAX, 0, 2}
};  // levelD


TEST(ExpiryTester, OverlapTests)
{
    bool flag;
    Version::FileMetaDataVector_t level1, level2, level_clear, expired_files;
    uint64_t now;
    ExpiryModuleEE module;
    VersionTester ver;
    const int overlap0(0), overlap1(1), sorted0(3), sorted1(4);
    VersionEdit edit;

    module.expiry_enabled=true;
    module.whole_file_expiry=true;
    module.expiry_minutes=2;

    now=port::TimeUint64();
    SetTimeMinutes(now);


    /** case: two levels, no overlap, no expiry **/
    CreateMetaArray(level1, levelA, 2);
    CreateMetaArray(level2, levelB, 2);
    ver.SetFileList(sorted0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, sorted0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(sorted0, level_clear);
    ver.SetFileList(sorted1, level_clear);

    ver.SetFileList(overlap0, level1);
    ver.SetFileList(overlap1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, overlap0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(overlap0, level_clear);
    ver.SetFileList(overlap1, level_clear);

    ver.SetFileList(overlap0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, overlap0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    ver.SetFileList(overlap0, level_clear);
    ver.SetFileList(sorted1, level_clear);

    /** case: two levels, 100% overlap, both levels expired **/
    SetTimeMinutes(now);
    CreateMetaArray(level1, levelC, 2);
    CreateMetaArray(level2, levelD, 2);
    SetTimeMinutes(now + 5*60000000);
    ver.SetFileList(sorted0, level1);
    ver.SetFileList(sorted1, level2);
    flag=module.CompactionFinalizeCallback(true, ver, sorted0, &edit);
    ASSERT_EQ(flag, false);
    ASSERT_EQ(edit.DeletedFileCount(), 0);
    flag=module.CompactionFinalizeCallback(true, ver, sorted1, &edit);
    ASSERT_EQ(flag, true);
    ASSERT_EQ(edit.DeletedFileCount(), 2);
    ver.SetFileList(sorted0, level_clear);
    ver.SetFileList(sorted1, level_clear);

    ClearMetaArray(level1);
    ClearMetaArray(level2);

}   // OverlapTests


enum eExpiryType
{
    eEXPIRY_NONE=1,
    eEXPIRY_AGED=2,
    eEXPIRY_EXPLICIT=3
};  // enum eExpiryType


struct sExpiryTestKey
{
    const char * m_Key;   // string key
    eExpiryType m_Type;   // type of expiry
    int m_NowMinus;       // expiry time to set
};


struct sExpiryTestFile
{
    // File size is generated
    int m_Number;
    int m_Level;               // level for file in manifest
    int m_LastValidState;      // in a "state" test, how long should this file be around
    sExpiryTestKey m_Keys[3];  // low, middle, high key
};


/**
 * Note:  constructor and destructor NOT called, this is
 *        an interface class only
 */

class ExpDB : public DBImpl
{
public:
    ExpDB(const Options& options, const std::string& dbname)
        : DBImpl(options, dbname) {}



    virtual ~ExpDB() {};

    VersionSet * GetVersionSet() {return(versions_);};
    const Options * GetOptions() {return(&options_);};

    void OneCompaction()
    {
        MutexLock l(&mutex_);
        MaybeScheduleCompaction();
        while (IsCompactionScheduled())
            bg_cv_.Wait();
    };  // OneCompaction

    void SetClock(uint64_t Time)
        {SetTimeMinutes(Time);};

    void ShiftClockMinutes(int Min)
    {
        uint64_t shift;

        shift=Min * 60 * port::UINT64_ONE_SECOND;
        SetTimeMinutes(GetTimeMinutes() + shift);
    };
};  // class ExpDB


class ExpTestModule : public ExpiryModuleEE
{
public:
    ExpTestModule() : m_ExpiryAllow(0), m_AllowLevel(-1) {};

    mutable int m_ExpiryAllow;
    mutable int m_AllowLevel;

    virtual bool CompactionFinalizeCallback(
        bool WantAll, const Version & Ver, int Level,
        VersionEdit * Edit) const
    {
        bool flag(false);

        if (0!=m_ExpiryAllow && NULL==Edit)
        {
            flag=ExpiryModuleEE::CompactionFinalizeCallback(WantAll, Ver, Level, Edit);

            if (flag)
            {
                m_AllowLevel=Level;
                -- m_ExpiryAllow;
            }   // if
        }   // if
        else if (-1!=m_AllowLevel && NULL!=Edit)
        {
            flag=ExpiryModuleEE::CompactionFinalizeCallback(WantAll, Ver, Level, Edit);

            if (flag)
            {
                m_AllowLevel=-1;
            }
        }   // else if

        return(flag);

    }   // CoompactionFinalizeCallback
};


class ExpiryManifestTester
{
public:
    ExpiryManifestTester()
        : m_Good(false), m_DB(NULL), m_Env(Env::Default()),
          m_BaseTime(port::TimeUint64()), m_Sequence(1)
    {
        m_DBName = test::TmpDir() + "/expiry";

        // clean up previous execution
        leveldb::DestroyDB(m_DBName, m_Options);

        m_Options.create_if_missing=true;
        m_Options.error_if_exists=false;
        m_Expiry=new ExpTestModule;
        m_Options.expiry_module=m_Expiry;

        OpenTestDB();
    };

    ~ExpiryManifestTester()
    {
        // clean up
        delete m_DB;
        leveldb::DestroyDB(m_DBName, m_Options);
    };

    bool m_Good;
    std::string m_DBName;
    Options m_Options;
    ExpTestModule * m_Expiry;
    Env * m_Env;
    ExpDB * m_DB;
    uint64_t m_BaseTime;
    SequenceNumber m_Sequence;

    void OpenTestDB()
    {
        leveldb::Status status;

        status=leveldb::DB::Open(m_Options, m_DBName, (DB**)&m_DB);

        m_Good=status.ok();
        ASSERT_OK(status);
        m_DB->SetClock(m_BaseTime);
    }   // OpenTestDB


    void CreateKey(const sExpiryTestKey & Key, InternalKey & Output)
    {
        ExpiryTime expiry;
        ValueType type;

        switch(Key.m_Type)
        {
            case(eEXPIRY_NONE):
                expiry=0;
                type=kTypeValue;
                break;

            case(eEXPIRY_AGED):
                expiry=m_BaseTime - Key.m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                type=kTypeValueWriteTime;
                break;

            case(eEXPIRY_EXPLICIT):
                expiry=m_BaseTime + Key.m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                type=kTypeValueExplicitExpiry;
                break;
        }   // switch

        ParsedInternalKey ikey(Key.m_Key, expiry, m_Sequence, type);

        Output.SetFrom(ikey);
        ++m_Sequence;
    }   // CreateKey


    void CreateFile(const sExpiryTestFile & File, VersionEdit & Edit)
    {
        std::string fname;
        Status s;
        WritableFile * outfile;
        TableBuilder * builder;
        InternalKey low_key, mid_key, high_key;
        uint64_t count1, count2, count3, file_size;

        fname = TableFileName(*m_DB->GetOptions(), File.m_Number, File.m_Level);
        s = m_Env->NewWritableFile(fname, &outfile, gMapSize);
        ASSERT_OK(s);
        builder = new TableBuilder(*m_DB->GetOptions(), outfile);

        CreateKey(File.m_Keys[0], low_key);
        CreateKey(File.m_Keys[1], mid_key);
        CreateKey(File.m_Keys[2], high_key);

        builder->Add(low_key.internal_key(), "Value");
        builder->Add(mid_key.internal_key(), "Value");
        builder->Add(high_key.internal_key(), "Value");

        s = builder->Finish();
        ASSERT_OK(s);

        count1=builder->GetExpiry1();
        count2=builder->GetExpiry2();
        count3=builder->GetExpiry3();

        s = outfile->Sync();
        ASSERT_OK(s);
        s = outfile->Close();
        ASSERT_OK(s);

        delete builder;
        delete outfile;

        m_Env->GetFileSize(fname, &file_size);

        Edit.AddFile2(File.m_Level, File.m_Number, file_size,
                       low_key, high_key,
                       count1, count2, count3);
    }    // CreateFile


    void CreateManifest(const sExpiryTestFile * Files, size_t Count)
    {
        int loop;
        const sExpiryTestFile * cursor;
        VersionEdit edit;
        port::Mutex mutex;
        Status s;

        m_Sequence=1;
        for (cursor=Files, loop=0; loop<Count; ++loop, ++cursor)
        {
            CreateFile(*cursor, edit);
        }   // for

        mutex.Lock();
        s=m_DB->GetVersionSet()->LogAndApply(&edit, &mutex);
        mutex.Unlock();
        ASSERT_OK(s);

    }   // CreateManifest


    void VerifyManifest(const sExpiryTestFile * Files, size_t Count)
    {
        const Version::FileMetaDataVector_t * file_list;
        Version::FileMetaDataVector_t::const_iterator it;
        int current_level, loop, loop1;
        const sExpiryTestFile * cursor;
        InternalKey low_key, mid_key, high_key;
        uint64_t expiry1, expiry2, expiry3, expires;

        // setup
        current_level=config::kNumLevels;
        file_list=NULL;
        m_Sequence=1;

        for (cursor=Files, loop=0; loop<Count; ++loop, ++cursor)
        {
            // get proper manifest level
            if (cursor->m_Level!=current_level)
            {
                current_level=cursor->m_Level;
                file_list=&m_DB->GetVersionSet()->current()->GetFileList(current_level);
                it=file_list->begin();
            }   // if

            // not set by builder   ASSERT_EQ((*it)->num_entries, 3);
            ASSERT_EQ((*it)->level, cursor->m_Level);

            // same code as above, just basic verification
            CreateKey(cursor->m_Keys[0], low_key);
            CreateKey(cursor->m_Keys[1], mid_key); // need to keep sequence # correct
            CreateKey(cursor->m_Keys[2], high_key);

            ASSERT_TRUE(0==m_Options.comparator->Compare(low_key.internal_key(),
                                                         (*it)->smallest.internal_key()));
            ASSERT_TRUE(0==m_Options.comparator->Compare(high_key.internal_key(),
                                                         (*it)->largest.internal_key()));

            // create our idea of the expiry settings
            expiry1=ULONG_MAX;
            expiry2=0;
            expiry3=0;

            for (loop1=0; loop1<3; ++loop1)
            {
                switch(cursor->m_Keys[loop1].m_Type)
                {
                    case eEXPIRY_NONE:
                        expiry1=0;
                        break;

                    case eEXPIRY_AGED:
                        expires=m_BaseTime - cursor->m_Keys[loop1].m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                        if (expires<expiry1)
                            expiry1=expires;
                        if (expiry2<expires)
                            expiry2=expires;
                        break;

                    case eEXPIRY_EXPLICIT:
                        expires=m_BaseTime + cursor->m_Keys[loop1].m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                        if (expiry3<expires)
                            expiry3=expires;
                        break;
                }   // switch
            }   // for

            // test our idea against manifest's idea
            ASSERT_EQ(expiry1, (*it)->expiry1);
            ASSERT_EQ(expiry2, (*it)->expiry2);
            ASSERT_EQ(expiry3, (*it)->expiry3);

            // inc here since not initialized upon for loop entry
            ++it;
        }   // for

        return;

    }   // VerifyManifest

    void VerifyFiles(const sExpiryTestFile * Files, size_t Count, int State)
    {
        int current_level, loop, loop1;
        std::vector<std::string> file_names;
        std::vector<std::string>::iterator f_it;

        std::string dir_name, target;
        const sExpiryTestFile * cursor;

        current_level=-1;

        for (cursor=Files, loop=0; loop<Count; ++loop, ++cursor)
        {
            if (cursor->m_Level!=current_level)
            {
                // should be no files left in list upon level change
                //   (except "." and "..")
                ASSERT_LE(file_names.size(), 2);
                file_names.clear();

                current_level=cursor->m_Level;
                dir_name=MakeDirName2(*m_DB->GetOptions(), current_level, "sst");
                m_Env->GetChildren(dir_name, &file_names);
            }   // if

            // is file still found on disk?
            if (State <= cursor->m_LastValidState)
            {
                // -2 omits directory
                target=TableFileName(*m_DB->GetOptions(), cursor->m_Number, -2);
                target.erase(0,target.find_last_of('/')+1);
                f_it=std::find(file_names.begin(), file_names.end(), target);
                ASSERT_TRUE(file_names.end()!=f_it);
                file_names.erase(f_it);
            }   // if
        }   // for

        // verify last populated level was good
        ASSERT_LE(file_names.size(), 2);

        return;

    }   // VerifyManifest


    void VerifyKeys(const sExpiryTestKey * Key, size_t Count, int Minutes)
    {
        Iterator * it;
        const sExpiryTestKey * cursor;
        int loop;

        it=m_DB->NewIterator(ReadOptions());
        it->SeekToFirst();

        for (cursor=Key, loop=0; loop<Count; ++cursor, ++loop)
        {

            if ( (eEXPIRY_EXPLICIT == cursor->m_Type && Minutes <= cursor->m_NowMinus)
                 || (eEXPIRY_AGED == cursor->m_Type && Minutes<m_Expiry->expiry_minutes))
            {
                ASSERT_TRUE(it->Valid());
                ASSERT_TRUE(0==strcmp(cursor->m_Key, it->key().ToString().c_str()));
                it->Next();
            }   // if
        }   // for

        delete it;

        return;

    }   // VerifyKeys


};  // ExpiryManifestTester


sExpiryTestFile Manifest1[]=
{
    {101, 6, 0, {{"02", eEXPIRY_NONE, 0}, {"05", eEXPIRY_NONE, 0}, {"07", eEXPIRY_NONE, 0}}},
    {102, 6, 0, {{"12", eEXPIRY_NONE, 0}, {"15", eEXPIRY_AGED, 25}, {"17", eEXPIRY_AGED, 25}}},
    {103, 6, 0, {{"22", eEXPIRY_AGED, 25}, {"25", eEXPIRY_EXPLICIT, 20}, {"27", eEXPIRY_EXPLICIT, 20}}},
    {104, 6, 0, {{"32", eEXPIRY_AGED, 25}, {"35", eEXPIRY_AGED, 25}, {"37", eEXPIRY_NONE, 0}}},
    {105, 6, 0, {{"42", eEXPIRY_AGED, 25}, {"45", eEXPIRY_NONE, 0}, {"47", eEXPIRY_AGED, 25}}},

    {201, 5, 0, {{"03", eEXPIRY_AGED, 10}, {"05", eEXPIRY_AGED, 10}, {"06", eEXPIRY_AGED, 10}}},
    {202, 5, 0, {{"11", eEXPIRY_NONE, 0}, {"15", eEXPIRY_EXPLICIT, 15}, {"18", eEXPIRY_EXPLICIT, 15}}},
    {203, 5, 0, {{"21", eEXPIRY_EXPLICIT, 15}, {"25", eEXPIRY_EXPLICIT, 15}, {"29", eEXPIRY_AGED, 10}}},
    {204, 5, 0, {{"34", eEXPIRY_EXPLICIT, 15}, {"35", eEXPIRY_EXPLICIT, 15}, {"39", eEXPIRY_NONE, 0}}},
    {205, 5, 0, {{"44", eEXPIRY_EXPLICIT, 15}, {"45", eEXPIRY_NONE, 0}, {"46", eEXPIRY_EXPLICIT, 15}}},

    {301, 4, 0, {{"03", eEXPIRY_EXPLICIT, 5}, {"05", eEXPIRY_EXPLICIT, 5}, {"06", eEXPIRY_EXPLICIT, 5}}},
    {302, 4, 0, {{"11", eEXPIRY_NONE, 0}, {"15", eEXPIRY_AGED, 5}, {"18", eEXPIRY_EXPLICIT, 5}}},
    {303, 4, 0, {{"21", eEXPIRY_EXPLICIT, 5}, {"25", eEXPIRY_AGED, 5}, {"29", eEXPIRY_EXPLICIT, 5}}},
    {304, 4, 0, {{"34", eEXPIRY_EXPLICIT, 5}, {"35", eEXPIRY_AGED, 5}, {"39", eEXPIRY_NONE, 0}}},
    {305, 4, 0, {{"44", eEXPIRY_AGED, 5}, {"45", eEXPIRY_NONE, 0}, {"46", eEXPIRY_EXPLICIT, 5}}}

};  // Manifest1

/**
 * Does manifest create correctly?
 */
TEST(ExpiryManifestTester, Manifest1)
{
    size_t manifest_count;
    Status s;

    manifest_count=sizeof(Manifest1) / sizeof(Manifest1[0]);
    CreateManifest(Manifest1, manifest_count);

    // quick verify
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(6), 5);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(5), 5);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(4), 5);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(3), 0);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(2), 0);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(1), 0);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(0), 0);

    // full verify
    VerifyManifest(Manifest1, manifest_count);

    // close, open, verify again
    delete m_DB;
    OpenTestDB();
    VerifyManifest(Manifest1, manifest_count);

    // close, repair, open, verify
    delete m_DB;
    s=RepairDB(m_DBName, m_Options);
    ASSERT_OK(s);
    OpenTestDB();
    VerifyManifest(Manifest1, manifest_count);

    return;
};


sExpiryTestFile Overlap1[]=
{
    // sorted levels
    {101, 6, 5, {{"02", eEXPIRY_NONE, 0}, {"05", eEXPIRY_NONE, 0}, {"07", eEXPIRY_NONE, 0}}},
    {102, 6, 2, {{"15", eEXPIRY_AGED, 25}, {"17", eEXPIRY_AGED, 25}, {"20", eEXPIRY_AGED, 25}}},

    {201, 5, 5, {{"22", eEXPIRY_NONE, 0}, {"24", eEXPIRY_NONE, 0}, {"25", eEXPIRY_NONE, 0}}},

    {301, 4, 5, {{"06", eEXPIRY_EXPLICIT, 5}, {"07", eEXPIRY_EXPLICIT, 5}, {"10", eEXPIRY_EXPLICIT, 5}}},
    {302, 4, 0, {{"35", eEXPIRY_EXPLICIT, 5}, {"37", eEXPIRY_EXPLICIT, 5}, {"40", eEXPIRY_EXPLICIT, 5}}},

    {401, 3, 5, {{"45", eEXPIRY_NONE, 0}, {"46", eEXPIRY_NONE, 0}, {"47", eEXPIRY_NONE, 0}}},

    {450, 2, 3, {{"11", eEXPIRY_AGED, 25}, {"17", eEXPIRY_AGED, 25}, {"21", eEXPIRY_AGED, 25}}},

    // Overlap levels
    {501, 1, 5, {{"10", eEXPIRY_AGED, 25}, {"17", eEXPIRY_AGED, 25}, {"23", eEXPIRY_AGED, 25}}},
    {502, 1, 5, {{"11", eEXPIRY_NONE, 0}, {"12", eEXPIRY_NONE, 0}, {"15", eEXPIRY_NONE, 0}}},
    {503, 1, 1, {{"33", eEXPIRY_AGED, 25}, {"34", eEXPIRY_AGED, 25}, {"42", eEXPIRY_AGED, 25}}}


};


/*
 * Test sequence that expired files get selected
 */
TEST(ExpiryManifestTester, Overlap1)
{
    size_t manifest_count;
    Status s;

    manifest_count=sizeof(Overlap1) / sizeof(Overlap1[0]);
    CreateManifest(Overlap1, manifest_count);

    // quick verify
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(6), 2);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(5), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(4), 2);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(3), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(2), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(1), 3);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(0), 0);

    // full verify
    VerifyManifest(Overlap1, manifest_count);
    VerifyFiles(Overlap1, manifest_count, 0);

    // enable compaction expiry
    m_Expiry->expiry_enabled=true;
    m_Expiry->expiry_minutes=60;
    m_Expiry->whole_file_expiry=true;

    m_DB->ShiftClockMinutes(10);
    m_Expiry->m_ExpiryAllow=1;
    m_DB->OneCompaction();
    VerifyFiles(Overlap1, manifest_count, 1);

    // total shift now 30 min
    m_DB->ShiftClockMinutes(30);
    m_Expiry->m_ExpiryAllow=1;
    m_DB->OneCompaction();
    VerifyFiles(Overlap1, manifest_count, 2);

    m_Expiry->m_ExpiryAllow=1;
    m_DB->OneCompaction();
    VerifyFiles(Overlap1, manifest_count, 3);

    m_Expiry->m_ExpiryAllow=1;
    m_DB->OneCompaction();
    VerifyFiles(Overlap1, manifest_count, 4);

    m_Expiry->m_ExpiryAllow=1;
    m_DB->OneCompaction();
    VerifyFiles(Overlap1, manifest_count, 5);

    return;
};


/*
 * Test compaction will find all without prompting
 */
TEST(ExpiryManifestTester, Overlap2)
{
    size_t manifest_count;
    Status s;

    manifest_count=sizeof(Overlap1) / sizeof(Overlap1[0]);
    CreateManifest(Overlap1, manifest_count);

    // quick verify
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(6), 2);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(5), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(4), 2);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(3), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(2), 1);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(1), 3);
    ASSERT_EQ(m_DB->GetVersionSet()->NumLevelFiles(0), 0);

    // full verify
    VerifyManifest(Overlap1, manifest_count);
    VerifyFiles(Overlap1, manifest_count, 0);

    // enable compaction expiry
    m_Expiry->expiry_enabled=true;
    m_Expiry->expiry_minutes=60;
    m_Expiry->whole_file_expiry=true;
    m_DB->ShiftClockMinutes(61);

    m_Expiry->m_ExpiryAllow=10;
    m_DB->OneCompaction();

    // let multiple threads complete
//    sleep(1);
    VerifyFiles(Overlap1, manifest_count, 5);

    return;
};


sExpiryTestKey Compact1[]=
{
    {"01", eEXPIRY_AGED, 0},
    {"02", eEXPIRY_EXPLICIT, 35},
    {"03", eEXPIRY_AGED, 0},
    {"04", eEXPIRY_EXPLICIT, 55},
    {"05", eEXPIRY_AGED, 0},
    {"06", eEXPIRY_EXPLICIT, 15},
    {"07", eEXPIRY_AGED, 0},
    {"08", eEXPIRY_EXPLICIT, 5},
    {"09", eEXPIRY_AGED, 0},
    {"10", eEXPIRY_EXPLICIT, 55},
    {"11", eEXPIRY_AGED, 0},
    {"12", eEXPIRY_EXPLICIT, 65},
    {"13", eEXPIRY_AGED, 0}

};


/*
 * Test expiry records get filtered during regular compaction
 *  (and expiring all leads to file deletion)
 */
TEST(ExpiryManifestTester, Compact1)
{
    size_t key_count;
    const sExpiryTestKey * Key;
    Status s;
    WriteBatch batch;
    KeyMetaData meta;
    int loop;
    ExpiryTime expiry;
    ValueType type;

    // enable compaction expiry
    m_Expiry->expiry_enabled=true;
    m_Expiry->expiry_minutes=30;
    m_Expiry->whole_file_expiry=false;

    key_count=sizeof(Compact1) / sizeof(Compact1[0]);

    for (loop=0, Key=Compact1; loop<key_count; ++loop, ++Key)
    {
        switch(Key->m_Type)
        {
            case(eEXPIRY_NONE):
                expiry=0;
                type=kTypeValue;
                break;

            case(eEXPIRY_AGED):
                expiry=m_BaseTime - Key->m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                type=kTypeValueWriteTime;
                break;

            case(eEXPIRY_EXPLICIT):
                expiry=m_BaseTime + Key->m_NowMinus * 60 * port::UINT64_ONE_SECOND;
                type=kTypeValueExplicitExpiry;
                break;
        }   // switch

        meta.m_Type=type;
        meta.m_Expiry=expiry;
        s=m_DB->Put(WriteOptions(), Key->m_Key, "gig\'em", &meta);
        ASSERT_OK(s);
    }   // for

    // load seem ok?
    VerifyKeys(Compact1, key_count, 0);

    // move write buffer to .sst file
    //  (no expiry in buffer to .sst conversion)
    m_DB->TEST_CompactMemTable();
    VerifyKeys(Compact1, key_count, 0);

    m_DB->ShiftClockMinutes(20);
    m_DB->TEST_CompactRange(3, NULL, NULL);
    VerifyKeys(Compact1, key_count, 20);

    m_DB->ShiftClockMinutes(16);
    m_DB->TEST_CompactRange(4, NULL, NULL);
    VerifyKeys(Compact1, key_count, 36);

    m_DB->ShiftClockMinutes(35);
    m_DB->TEST_CompactRange(5, NULL, NULL);
    VerifyKeys(Compact1, key_count, 71);

}   // Compact1


}  // namespace leveldb

