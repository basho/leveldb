// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

#include "port/port.h"
#include "util/testharness.h"
#include "util/classes/StatManager.h"

namespace leveldb {

  class ClassTest {
  public:
  };


  TEST(ClassTest, StatManagerTest) {

    leveldb::util::StatManager sm(1,20);
    sm.spawnStrobeThread();

    std::map<std::string, uint64_t> addMap;

    addMap.clear();

    unsigned nTest=3;
    for(unsigned i=0; i < nTest; i++) {
      addMap["test1"] = 1;
      sm.add(addMap);
      sleep(1);
    }

#if 0
    addMap.clear();

    for(unsigned i=0; i < 2; i++) {

      addMap["test2"] = 2;
      addMap["test3"] = 3;

      sm.add(addMap);
      sleep(1);
    }
#endif

    COUT("Retrieving counts");

    // Retrieve counts

    std::map<std::string, leveldb::util::Sample> samples; 
    samples["test1"];
    sm.getCounts(samples);

    COUT(sm.formatOutput("test", samples));

    ASSERT_TRUE(samples["test1"].total_ == nTest);
    ASSERT_TRUE(samples["test1"].differentials_.size() > 0);
  }

}  // namespace leveldb

int main(int argc, char** argv) {
  return leveldb::test::RunAllTests();
}
