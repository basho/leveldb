// -------------------------------------------------------------------
//
// penalty_test.cc
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


#include "util/testharness.h"
#include "util/testutil.h"

#include "leveldb/comparator.h"

#include "db/version_set.h"

/**
 * Execution routine
 */
int main(int argc, char** argv)
{
  return leveldb::test::RunAllTests();
}


namespace leveldb {

class TestVersion : public Version    
{
public:
    TestVersion()
        : Version(NULL)
    {
        int loop;

        for (loop=0; loop<config::kNumLevels; ++loop)
        {
            m_FalseFile[loop].file_size=0;
            m_LevelFileCount[loop]=0;
        }   // for
    };
    
    virtual size_t NumFiles(int level) const {return(m_LevelFileCount[level]);};

    virtual const std::vector<FileMetaData*> & GetFileList(int level) const
    {
        m_FalseVector.clear();
        m_FalseVector.push_back(&m_FalseFile[level]);
        return(m_FalseVector);
    };
    
    mutable std::vector<FileMetaData*> m_FalseVector;
    mutable FileMetaData m_FalseFile[config::kNumLevels];

    size_t m_LevelFileCount[config::kNumLevels];
    
};  // class TestVersion

/**
 * Wrapper class for tests.  Holds working variables
 * and helper functions.
 */
class PenaltyTester : public VersionSet
{
public:
    PenaltyTester()
        : m_IntCompare(m_Options.comparator), VersionSet("", &m_Options, NULL, &m_IntCompare)
    {
    };

    ~PenaltyTester()
    {
    };

    Options m_Options;
    InternalKeyComparator m_IntCompare;
    
};  // class PenaltyTester


/**
 * Debug 1
 */
TEST(PenaltyTester, Debug1)
{
    TestVersion version;
    int penalty;

    m_Options.write_buffer_size=46416847;

    version.m_FalseFile[2].file_size=1075676398;
    version.m_LevelFileCount[1]=1;
    
    UpdatePenalty(&version);

    ASSERT_EQ(version.WritePenalty(), 0);

    //version.m_FalseFile[3].file_size=;
    
    
}   // test Debug1


/**
 * No penalty
 */
TEST(PenaltyTester, NoPenalty)
{
    TestVersion version;
    int penalty;

    m_Options.write_buffer_size=46416847;

    // nothing
    UpdatePenalty(&version);
    ASSERT_EQ(version.WritePenalty(), 0);

    // level 0, no penalty 
    version.m_LevelFileCount[0]=config::kL0_CompactionTrigger;
    UpdatePenalty(&version);
    ASSERT_EQ(version.WritePenalty(), 0);

    version.m_LevelFileCount[0]=config::kL0_SlowdownWritesTrigger;
    UpdatePenalty(&version);
    ASSERT_EQ(version.WritePenalty(), 0);
    
    // threshold reached ... some penaltyc
    version.m_LevelFileCount[0]=config::kL0_SlowdownWritesTrigger+1;
    UpdatePenalty(&version);
    ASSERT_NE(version.WritePenalty(), 0);
    

    //version.m_FalseFile[2].file_size=1075676398;
    //version.m_FalseFile[3].file_size=;
    
    
}   // test NoPenalty

    

}  // namespace leveldb
