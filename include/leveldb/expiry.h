// -------------------------------------------------------------------
//
// expiry.h:  background expiry management for Basho's modified leveldb
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

#ifndef EXPIRY_H
#define EXPIRY_H

#include <stdint.h>
#include "leveldb/env.h"
#include "leveldb/options.h"

namespace leveldb {

class Logger;
class Slice;
class SstCounters;
class ParsedInternalKey;
class Version;
class Compaction;

class ExpiryModule
{
public:
    ExpiryModule() {};
    ~ExpiryModule() {};

    // Print expiry options to LOG file
    virtual void Dump(Logger * log) const
    {Log(log,"                        Expiry: (none)");};

    // db/write_batch.cc MemTableInserter::Put() calls this.
    // returns false on internal error
    virtual bool MemTableInserterCallback(
        const Slice & Key,   // input: user's key about to be written
        const Slice & Value, // input: user's value object
        ValueType & ValType,   // input/output: key type. call might change
        ExpiryTime & Expiry) const  // input/output: 0 or specific expiry. call might change
    {return(true);};

    // db/dbformat.cc KeyRetirement::operator() calls this.
    // db/version_set.cc SaveValue() calls this too.
    // returns true if key is expired, returns false if key not expired
    virtual bool KeyRetirementCallback(
        const ParsedInternalKey & Ikey) const
    {return(false);};

    // table/table_builder.cc TableBuilder::Add() calls this.
    // returns false on internal error
    virtual bool TableBuilderCallback(
        const Slice & Key,       // input: internal key
        SstCounters & Counters) const // input/output: counters for new sst table
    {return(true);};

    // db/memtable.cc MemTable::Get() calls this.
    // returns true if type/expiry is expired, returns false if not expired
    virtual bool MemTableCallback(
        ValueType Type,                  // input: ValueType from key
        const ExpiryTime & Expiry) const // input: Expiry from key, or zero
    {return(false);};

    // db/version_set.cc VersionSet::Finalize() calls this if no
    //  other compaction selected for a level
    // returns true if there is an expiry compaction eligible
    virtual bool CompactionFinalizeCallback(
        Version * current,     // input/output: Version considering compaction
        const int level) const // input: ... at this level
    {return(false);};

    virtual void ExpiryBackgroundCall(
        Compaction * Compact) const  // input: ... data for compaction
    {};


};  // ExpiryModule


} // namespace leveldb

#endif // ifndef

