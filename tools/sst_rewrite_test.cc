// -------------------------------------------------------------------
//
// sst_rewrite_test.cc
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

#include "rewrite_state.h"


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
class OptionsIterTester
{
public:
    OptionsIterTester()
    {
    };

    ~OptionsIterTester()
    {
    };
};  // class OptionsIterTester



/**
 * Quick test of OptionsIter moving forward and backward
 */
TEST(OptionsIterTester, Basics)
{
    OptionsIter iter1;
    const char * array1[]={"one", "two", "three", "four", NULL};

    iter1=array1;
    ASSERT_TRUE(0==strcmp("one", *iter1));
    ++iter1;
    ASSERT_TRUE(0==strcmp("two", *iter1));
    ++iter1;
    ASSERT_TRUE(0==strcmp("three", *iter1));
    ++iter1;
    ASSERT_TRUE(0==strcmp("four", *iter1));
    ++iter1;
    ASSERT_TRUE(NULL == *iter1);
    --iter1;
    ASSERT_TRUE(0==strcmp("four", *iter1));
    --iter1;
    ASSERT_TRUE(0==strcmp("three", *iter1));
    --iter1;
    ASSERT_TRUE(0==strcmp("two", *iter1));
    --iter1;
    ASSERT_TRUE(0==strcmp("one", *iter1));

    ASSERT_TRUE(0==strcmp("two", ++iter1));
    ASSERT_TRUE(0==strcmp("three", ++iter1));
    ASSERT_TRUE(0==strcmp("four", ++iter1));
    ASSERT_TRUE(0==strcmp("three", --iter1));
    ASSERT_TRUE(0==strcmp("two", --iter1));
    ASSERT_TRUE(0==strcmp("one", --iter1));

}   // OptionsIterTester::Basics




}  // namespace leveldb

