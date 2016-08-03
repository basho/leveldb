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

#include "util/testharness.h"
#include "util/testutil.h"

#include "db/dbformat.h"
#include "db/db_impl.h"
#include "util/hot_backup.h"


/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {



/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class HotBackupTester
{
public:
    HotBackupTester()
    {
    };

    ~HotBackupTester()
    {
    };
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
    unlink(config::kTriggerFileName);

    // does parent path exist?
    dup_path=strdup(config::kTriggerFileName);
    path=dirname(dup_path);
    if (0!=access(path, F_OK))
    {
        // assumes only one path level needed
        //  (and that we are allowed to create it, unlikely in /etc)
        ret_val=mkdir(path, 0666);
        ASSERT_EQ(0, ret_val);
    }   // if
    free(dup_path);
    dup_path=NULL;

    // is a trigger seen (hope not)
    ret_flag=IsHotBackupTriggerSet();
    ASSERT_TRUE(!ret_flag);

    // make a trigger
    ret_val=open(config::kTriggerFileName, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    ASSERT_TRUE(-1!=ret_val);
    close(ret_val);

    // test the trigger
    ret_flag=IsHotBackupTriggerSet();
    ASSERT_TRUE(ret_flag);

    // pretend to be back process
    /// schedule twice
    HotBackupScheduled();
    HotBackupScheduled();

    // trigger still there?
    ret_flag=IsHotBackupTriggerSet();
    ASSERT_TRUE(ret_flag);

    // release one, trigger goes away
    HotBackupFinished();
    ret_flag=IsHotBackupTriggerSet();
    ASSERT_TRUE(!ret_flag);

    // did our simulation create a syslog entry?
    //  (bonus if you manually check /var/log/syslog for actual entries)
    perf_after=gPerfCounters->Value(ePerfSyslogWrite) - perf_before;
    ASSERT_TRUE( 1==perf_after );

    // clean up
    HotBackupFinished();

}   // FileTriggerTest




}  // namespace leveldb

