// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

// env.h djusted for riak

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_RIAK_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_RIAK_H_

#include <leveldb/env.h>

namespace leveldb {

static Env* RiakDefaultEnv(unsigned ThreadBlocks=0);


extern pthread_rwlock_t gThreadLock0;
extern pthread_rwlock_t gThreadLock1;

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ENV_H_
