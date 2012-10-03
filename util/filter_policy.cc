// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

namespace leveldb {

FilterPolicy::~FilterPolicy() { }

Slice FilterPolicy::TransformKey(const Slice & Key, std::string & Buffer) const
  {return(Key);};

}  // namespace leveldb
