// -------------------------------------------------------------------
//
// hot_backup_test.cc
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

#include <fcntl.h>
#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "leveldb/db.h"
#include "util/testharness.h"
#include "util/testutil.h"

#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "util/hot_backup.h"


/**
 * Execution routine
 */
int main(int argc, char** argv)
{
    int ret_val;

    ret_val=leveldb::test::RunAllTests();

    return(ret_val);
}   // main


namespace leveldb {


/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class HotBackupTester : public HotBackup
{
public:
    std::string m_DBName;
    std::string m_Trigger;

    HotBackupTester()
        : HotBackup(this)
    {
        m_DBName = test::TmpDir() + "/hot_backup";
        m_Trigger = test::TmpDir() + "/trigger";
    };

    virtual ~HotBackupTester()
    {
        ResetHotBackupObject();
    };

    virtual const char * GetTriggerPath() {return(m_Trigger.c_str());};

    void ClearAllBackups(
        Options & InOptions)
    {
        int loop;
        Options options;

        // clear old if exists
        for (loop=0; loop<=config::kNumBackups; ++loop)
        {
            options=InOptions;

            SetBackupPaths(options, loop);
            DestroyDB("", options);
        }   // if

        options.tiered_slow_level=4;
        options.tiered_fast_prefix=m_DBName + "/fast";
        options.tiered_slow_prefix=m_DBName + "/slow";
        DestroyDB("", options);
    }   // ClearAllBackups

};  // class HotBackupTester


/**
 * Initial code used the existance of /etc/riak/hot_backup file
 *  as flag to start a backup.
 */
TEST(HotBackupTester, FileTriggerTest)
{
    char * dup_path, *path;
    int ret_val;
    bool ret_flag;
    uint64_t perf_before, perf_after;

    perf_before=gPerfCounters->Value(ePerfSyslogWrite);

    // cleanup anything existing, likely fails
    ///  hmm, should there be a way to move this trigger
    ///  to a "safe" area like /tmp?
    unlink(GetTriggerPath());

    // does parent path exist?
    //  bypass test if it does not ... Travis CI and
    //  other users might not be able to access /etc/riak
    dup_path=strdup(GetTriggerPath());
    path=dirname(dup_path);
    ret_val=access(path, R_OK | W_OK);
    ASSERT_TRUE(-1!=ret_val);

    // is a trigger seen (hope not)
    ret_flag=HotBackup::IsTriggerSet();
    ASSERT_TRUE(!ret_flag);

    // make a trigger
    ret_val=open(GetTriggerPath(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(-1!=ret_val);
    close(ret_val);

    // test the trigger
    ret_flag=HotBackup::IsTriggerSet();
    ASSERT_TRUE(ret_flag);

    // pretend to be back process
    /// schedule twice
    HotBackup::HotBackupScheduled();
    HotBackup::HotBackupScheduled();

    // trigger still there?
    ret_flag=HotBackup::IsTriggerSet();
    ASSERT_TRUE(ret_flag);

    // release one, trigger goes away
    HotBackup::HotBackupFinished();
    ret_flag=HotBackup::IsTriggerSet();
    ASSERT_TRUE(!ret_flag);

    // did our simulation create a syslog entry?
    //  (bonus if you manually check /var/log/syslog for actual entries)
    perf_after=gPerfCounters->Value(ePerfSyslogWrite) - perf_before;
    ASSERT_TRUE( 1==perf_after );

    // clean up second count.
    HotBackupFinished();

    // clean up
    free(dup_path);
    dup_path=NULL;

}   // FileTriggerTest

/**
 * Verify that backup directories rotate
 */
TEST(HotBackupTester, DirectoryRotationTest)
{
    int ret_val, loop, inner_loop, offset, file_num;
    Options options, backup_options, inner_options;
    bool ret_flag, should_find, did_find;
    std::string table_file, backup_name;

    options.tiered_slow_level=4;
    options.tiered_fast_prefix=m_DBName + "/fast";
    options.tiered_slow_prefix=m_DBName + "/slow";
    ClearAllBackups(options);

    // manually create database directories
    ret_val=mkdir(m_DBName.c_str(), 0777);
    ret_val=mkdir(options.tiered_fast_prefix.c_str(), 0777);
    ASSERT_TRUE(0==ret_val);
    ret_val=mkdir(options.tiered_slow_prefix.c_str(), 0777);
    ASSERT_TRUE(0==ret_val);
    MakeLevelDirectories(Env::Default(), options);

    backup_options=options;
    SetBackupPaths(backup_options, 0);

    // this loop goes one higher than permitted retention
    //  to validate deletion of oldest
    for (loop=0; loop<=config::kNumBackups; ++loop)
    {
        // rotate directories
        ret_flag=PrepareDirectories(options);
        ASSERT_TRUE(ret_flag);

        // these files are to "mark" the backups, not
        //  pretending this is a true file link
        // make a file in fast tier ... 10 + backup iteration
        table_file=TableFileName(backup_options, 10+loop, 1);
        ret_val=open(table_file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
        ASSERT_TRUE(-1!=ret_val);
        close(ret_val);

        // make a file in slow tier ... 20 + backup iteration
        table_file=TableFileName(backup_options, 20+loop, 5);
        ret_val=open(table_file.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
        ASSERT_TRUE(-1!=ret_val);
        close(ret_val);

        // test files
        for (inner_loop=config::kNumBackups+1; 0<=inner_loop; --inner_loop)
        {
            offset=(loop<config::kNumBackups) ? 0 : loop-config::kNumBackups +1;
            should_find=(inner_loop<=loop) && (offset<=inner_loop);

            inner_options=options;
            SetBackupPaths(inner_options, inner_loop-offset);

            file_num=loop - inner_loop + offset;

            table_file=TableFileName(inner_options, 10+file_num, 1);
            ret_val=access(table_file.c_str(), F_OK);
            ASSERT_TRUE( (0==ret_val) == should_find);

            table_file=TableFileName(inner_options, 20+file_num, 5);
            ret_val=access(table_file.c_str(), F_OK);
            ASSERT_TRUE( (0==ret_val) == should_find);
        }   // for
    }   // for

    ClearAllBackups(options);

    // clean up ... other unit test programs leave stuff, this fails
    ret_val=rmdir(m_DBName.c_str());
    //ASSERT_TRUE(0==ret_val);

}   // DirectoryRotationTest


/**
 * Test that code has access permissions to copy
 * live LOG file.
 */
TEST(HotBackupTester, LOGCopyTest)
{
    Options options;
    Status s;
    DB * db;
    WriteOptions write_ops;
    std::string backup_path, backup_LOG;
    long log_position;
    bool ret_flag;
    struct stat file_stat;
    int ret_val;

    backup_path=BackupPath(m_DBName, 0);
    backup_LOG=InfoLogFileName(backup_path);

    // preclean
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(m_DBName, options);
    rmdir(m_DBName.c_str());

    // need a live database for this test.
    options.create_if_missing=true;
    options.error_if_exists=false;
    s=DB::Open(options, m_DBName, &db);
    ASSERT_OK(s);

    // where is log now
    log_position=db->GetLogger()->LogSize();

    // add to the log
    Log(db->GetLogger(), "LOGCopyTest line 1");
    Log(db->GetLogger(), "LOGCopyTest line 2");

    // create destination path manually
    s=Env::Default()->CreateDir(backup_path.c_str());
    ASSERT_OK(s);

    // Test the routine
    ret_flag=((DBImpl*)db)->CopyLOGSegment(log_position);
    ASSERT_TRUE(ret_flag);

    // Test size of destination file
    ret_val=lstat(backup_LOG.c_str(), &file_stat);
    ASSERT_TRUE(0==ret_val);
    ASSERT_TRUE(log_position==file_stat.st_size);

    // clean up
    delete db;
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(m_DBName, options);
    rmdir(m_DBName.c_str());

}   // LOGCopyTest


/**
 * Test contents of each backup database
 */
TEST(HotBackupTester, ContentReviewTest)
{
    Options options;
    Status s;
    DB * db;
    WriteOptions write_ops;
    ReadOptions read_ops;
    std::string backup_path, out_buffer;

    // preclean
    backup_path=BackupPath(m_DBName, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());

    backup_path=BackupPath(m_DBName, 1);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());

    DestroyDB(m_DBName, options);
    rmdir(m_DBName.c_str());

    // need a live database for this test.
    options.create_if_missing=true;
    options.error_if_exists=false;
    s=DB::Open(options, m_DBName, &db);
    ASSERT_OK(s);

    // little bit of data
    s=db->Put(write_ops, "10", "apple");
    ASSERT_OK(s);
    s=db->Put(write_ops, "12", "banana");
    ASSERT_OK(s);
    s=db->Put(write_ops, "14", "canalope");
    ASSERT_OK(s);
    s=db->Put(write_ops, "16", "date");
    ASSERT_OK(s);
    s=db->Put(write_ops, "18", "egg plant");
    ASSERT_OK(s);

    // first backup
    //  pretend it came via CheckHotBackupTrigger
    HotBackupScheduled();
    HotBackupTask task((DBImpl*)db);

    task();

    // now add 5 more and try again
    s=db->Put(write_ops, "21", "ardvark");
    ASSERT_OK(s);
    s=db->Put(write_ops, "23", "bunny");
    ASSERT_OK(s);
    s=db->Put(write_ops, "25", "cat");
    ASSERT_OK(s);
    s=db->Put(write_ops, "27", "dog");
    ASSERT_OK(s);
    s=db->Put(write_ops, "29", "eagle");
    ASSERT_OK(s);

    // second backup
    //  pretend it came via CheckHotBackupTrigger
    HotBackupScheduled();

    task();

    // now add 5 more that will not backup
    s=db->Put(write_ops, "70.3", "half iron");
    ASSERT_OK(s);
    s=db->Put(write_ops, "140.6", "full iron");
    ASSERT_OK(s);
    s=db->Put(write_ops, "26.2", "marathon");
    ASSERT_OK(s);
    s=db->Put(write_ops, "13.1", "half marathon");
    ASSERT_OK(s);
    s=db->Put(write_ops, "0", "did not finish");
    ASSERT_OK(s);

    // sample the three sets
    s=db->Get(read_ops, "13.1", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "27", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "16", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "14", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "25", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "26.2", &out_buffer);
    ASSERT_OK(s);
    // test fail case
    s=db->Get(read_ops, "26.1", &out_buffer);
    ASSERT_TRUE(!s.ok());

    // close parent
    delete db;
    db=NULL;

    // open most recent backup
    backup_path=BackupPath(m_DBName, 0);
    s=DB::Open(options, backup_path, &db);
    ASSERT_OK(s);

    // should have data from first two sets, but not third
    s=db->Get(read_ops, "21", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "10", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "70.3", &out_buffer);
    ASSERT_TRUE(!s.ok());

    // close backup
    delete db;
    db=NULL;

    // first backup
    backup_path=BackupPath(m_DBName, 1);
    s=DB::Open(options, backup_path, &db);
    ASSERT_OK(s);

    // should have data from first set only
    s=db->Get(read_ops, "18", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "29", &out_buffer);
    ASSERT_TRUE(!s.ok());
    s=db->Get(read_ops, "26.2", &out_buffer);
    ASSERT_TRUE(!s.ok());

    // close backup
    delete db;
    db=NULL;

    // test links ... get rid of parent, retry backup
    DestroyDB(m_DBName, options);

    backup_path=BackupPath(m_DBName, 1);
    s=DB::Open(options, backup_path, &db);
    ASSERT_OK(s);

    // should have data from first set only
    s=db->Get(read_ops, "18", &out_buffer);
    ASSERT_OK(s);
    s=db->Get(read_ops, "29", &out_buffer);
    ASSERT_TRUE(!s.ok());
    s=db->Get(read_ops, "26.2", &out_buffer);
    ASSERT_TRUE(!s.ok());

    // close backup
    delete db;
    db=NULL;

    // clean up all
    backup_path=BackupPath(m_DBName, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());

    backup_path=BackupPath(m_DBName, 1);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());

    DestroyDB(m_DBName, options);
    rmdir(m_DBName.c_str());

}   // ContentReviewTest


/**
 * Verify two DBs will backup when
 *  request issued via trigger
 */
TEST(HotBackupTester, DoubleDB)
{
    Options options;
    Status s;
    DB * db1, * db2;
    WriteOptions write_ops;
    ReadOptions read_ops;
    std::string db_path, backup_path, out_buffer, sst_path;
    int ret_val, count;

    // preclean
    unlink(GetTriggerPath());

    db_path=m_DBName + "/db1";
    backup_path=BackupPath(db_path, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(db_path, options);
    rmdir(db_path.c_str());

    db_path=m_DBName + "/db2";
    backup_path=BackupPath(db_path, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(db_path, options);
    rmdir(db_path.c_str());

    // need live databases for this test.
    options.create_if_missing=true;
    options.error_if_exists=false;

    mkdir(m_DBName.c_str(), 0777);
    db_path=m_DBName + "/db1";
    s=DB::Open(options, db_path, &db1);
    ASSERT_OK(s);

    db_path=m_DBName + "/db2";
    s=DB::Open(options, db_path, &db2);
    ASSERT_OK(s);

    // 5 to first db
    s=db1->Put(write_ops, "10", "apple");
    ASSERT_OK(s);
    s=db1->Put(write_ops, "12", "banana");
    ASSERT_OK(s);
    s=db1->Put(write_ops, "14", "canalope");
    ASSERT_OK(s);
    s=db1->Put(write_ops, "16", "date");
    ASSERT_OK(s);
    s=db1->Put(write_ops, "18", "egg plant");
    ASSERT_OK(s);

    // 5 to second db
    s=db2->Put(write_ops, "21", "ardvark");
    ASSERT_OK(s);
    s=db2->Put(write_ops, "23", "bunny");
    ASSERT_OK(s);
    s=db2->Put(write_ops, "25", "cat");
    ASSERT_OK(s);
    s=db2->Put(write_ops, "27", "dog");
    ASSERT_OK(s);
    s=db2->Put(write_ops, "29", "eagle");
    ASSERT_OK(s);

    // set trigger
    ret_val=open(GetTriggerPath(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(-1!=ret_val);
    close(ret_val);

    // cheat, play role of throttle loop and test for trigger
    CheckHotBackupTrigger();

    // wait for backup to process, but only 10 seconds
    count=0;
    do
    {
        sleep(1);
        ++count;
    } while(0==access(GetTriggerPath(), F_OK) && count<10);
    ASSERT_TRUE(count<10);

    // expected .sst file exist?
    db_path=m_DBName + "/db1";
    backup_path=BackupPath(db_path,0);
    options.tiered_slow_prefix=backup_path;
    sst_path=TableFileName(options, 5, 3);
    ret_val=access(sst_path.c_str(), F_OK);
    ASSERT_TRUE(0==ret_val);

    db_path=m_DBName + "/db2";
    backup_path=BackupPath(db_path,0);
    options.tiered_slow_prefix=backup_path;
    sst_path=TableFileName(options, 5, 3);
    ret_val=access(sst_path.c_str(), F_OK);
    ASSERT_TRUE(0==ret_val);

    // cleanup
    delete db1;
    delete db2;

    db_path=m_DBName + "/db1";
    backup_path=BackupPath(db_path, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(db_path, options);
    rmdir(db_path.c_str());

    db_path=m_DBName + "/db2";
    backup_path=BackupPath(db_path, 0);
    DestroyDB(backup_path, options);
    rmdir(backup_path.c_str());
    DestroyDB(db_path, options);
    rmdir(db_path.c_str());


}   // DoubleDBTest

}   // namespace leveldb

