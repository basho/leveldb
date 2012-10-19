// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/options.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "util/crc32c.h"

namespace leveldb {

Options::Options()
    : comparator(BytewiseComparator()),
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()),
      info_log(NULL),
      write_buffer_size(4<<20),
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL)
{
}


void
Options::Dump(
    Logger * log) const
{
    Log(log,"            Options.comparator: %s", comparator->Name());
    Log(log,"     Options.create_if_missing: %d", create_if_missing);
    Log(log,"       Options.error_if_exists: %d", error_if_exists);
    Log(log,"       Options.paranoid_checks: %d", paranoid_checks);
    Log(log,"                   Options.env: %p", env);
    Log(log,"              Options.info_log: %p", info_log);
    Log(log,"     Options.write_buffer_size: %zd", write_buffer_size);
    Log(log,"        Options.max_open_files: %d", max_open_files);
    Log(log,"           Options.block_cache: %p", block_cache);
    Log(log,"            Options.block_size: %zd", block_size);
    Log(log,"Options.block_restart_interval: %d", block_restart_interval);
    Log(log,"           Options.compression: %d", compression);
    Log(log,"         Options.filter_policy: %s", filter_policy == NULL ? "NULL" : filter_policy->Name());
    Log(log,"                        crc32c: %s", crc32c::IsHardwareCRC() ? "hardware" : "software");
}   // Options::Dump

}  // namespace leveldb
