// -------------------------------------------------------------------
//
// perf_count.h:  performance counters LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
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

#ifndef STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_
#define STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_

#include <stdint.h>
#include <string>
#include "leveldb/status.h"

namespace leveldb {

enum SstCountEnum
{
    //
    // array index values/names
    //
    eSstCountKeys=0,           //!< how many keys in this sst
    eSstCountBlocks=1,         //!< how many blocks in this sst
    eSstCountCompressAborted=2,//!< how many blocks attempted compression and aborted use
    eSstCountKeySize=3,        //!< byte count of all keys
    eSstCountValueSize=4,      //!< byte count of all values
    eSstCountBlockSize=5,      //!< byte count of all blocks (pre-compression)
    eSstCountBlockWriteSize=6, //!< post-compression size, or BlockSize if no compression
    eSstCountIndexKeys=7,      //!< how many keys in the index block

    // must follow last index name to represent size of array
    eSstCountEnumSize,          //!< size of the array described by the enum values

    eSstCountVersion=1

};  // enum SstCountEnum


class SstCounters
{
protected:
    bool m_IsReadOnly;         //!< set when data decoded from a file
    uint32_t m_Version;        //!< object revision identification
    uint32_t m_CounterSize;    //!< number of objects in m_Counter

    uint64_t m_Counter[eSstCountEnumSize];

public:
    // constructors / destructor
    SstCounters();

    // Put data into disk form
    void EncodeTo(std::string & Dst) const;

    // Populate member data from prior EncodeTo block
    Status DecodeFrom(const Slice& src);

    // increment the counter
    uint64_t Inc(unsigned Index);

    // add value to the counter
    uint64_t Add(unsigned Index, uint64_t Amount);

    // return value of a counter
    uint64_t Value(unsigned Index);

    // return number of counters
    uint32_t Size() const {return(m_CounterSize);};

    // printf all values
    void Dump();

};  // class SstCounters


extern struct PerformanceCounters * gPerfCounters;

struct PerformanceCounters
{
    enum
    {
        eVersion=1,            //!< structure versioning
    };


    volatile uint32_t m_StructSize;     //!< part 1 of object revision identification
    volatile uint32_t m_Version;        //!< part 2 of object revision identification

    volatile uint64_t m_ROFileOpen;     //!< PosixMmapReadableFile open
    volatile uint64_t m_ROFileClose;    //!<  closed
    volatile uint64_t m_ROFileUnmap;    //!<  unmap without close

    volatile uint64_t m_RWFileOpen;     //!< PosixMmapFile open
    volatile uint64_t m_RWFileClose;    //!<  closed
    volatile uint64_t m_RWFileUnmap;    //!<  unmap without close

    volatile uint64_t m_ApiOpen;        //!< Count of DB::Open completions
    volatile uint64_t m_ApiGet;         //!< Count of DBImpl::Get completions
    volatile uint64_t m_ApiWrite;       //!< Count of DBImpl::Get completions

    volatile uint64_t m_WriteSleep;     //!< DBImpl::MakeRoomForWrite called sleep
    volatile uint64_t m_WriteWaitImm;   //!< DBImpl::MakeRoomForWrite called Wait on Imm compact
    volatile uint64_t m_WriteWaitLevel0;//!< DBImpl::MakeRoomForWrite called Wait on Level0 compact
    volatile uint64_t m_WriteNewMem;    //!< DBImpl::MakeRoomForWrite created new memory log
    volatile uint64_t m_WriteError;     //!< DBImpl::MakeRoomForWrite saw bg_error_
    volatile uint64_t m_WriteNoWait;    //!< DBImpl::MakeRoomForWrite took no action

    volatile uint64_t m_GetMem;         //!< DBImpl::Get read from memory log
    volatile uint64_t m_GetImm;         //!< DBImpl::Get read from previous memory log
    volatile uint64_t m_GetVersion;     //!< DBImpl::Get read from Version object

    volatile uint64_t m_SearchLevel[7]; //!< Version::Get read searched one or more files here

    volatile uint64_t m_TableCached;    //!< TableCache::FindTable found table in cache
    volatile uint64_t m_TableOpened;    //!< TableCache::FindTable had to open table file
    volatile uint64_t m_TableGet;       //!< TableCache::Get used to retrieve a key

    volatile uint64_t m_BGCloseUnmap;   //!< PosixEnv::BGThreaed started Unmap/Close job
    volatile uint64_t m_BGCompactImm;   //!< PosixEnv::BGThreaed started compaction of Imm or Level0
    volatile uint64_t m_BGNormal;       //!< PosixEnv::BGThreaed started normal compaction job

    volatile uint64_t m_BlockFiltered;  //!< Table::BlockReader search stopped due to filter
    volatile uint64_t m_BlockFilterFalse;//!< Table::BlockReader gave a false positive for match
    volatile uint64_t m_BlockCached;    //!< Table::BlockReader found block in cache
    volatile uint64_t m_BlockRead;      //!< Table::BlockReader read block from disk
    volatile uint64_t m_BlockFilterRead;//!< Table::ReadMeta filter loaded from file
    volatile uint64_t m_BlockValidGet;  //!< Table::InternalGet has valid iterator

    volatile uint64_t m_Debug[5];       //!< Developer debug counters, moveable

    //!< does executable's idea of version match shared object?
    bool VersionTest()
        {return(sizeof(PerformanceCounters)==m_StructSize && eVersion==m_Version);};

    void Init();

    void Dump();

#if 0
    // there is no CloseSharedMemFile at this time.
    //  --> really need a Manager object that holds pointer, static struct,
    //      and fd for process
    static bool
    OpenSharedMemFile(
        const char * FileName)
        {
            bool good;
            int fd;

            good=false;
            fd = open(FileName, O_CREAT | O_RDWR, 0644);
            if (-1!=fd)
            {
                void * base;
                int ret_val;

                base=MAP_FAILED;

                ret_val=ftruncate(fd, sizeof(PerformanceCounters));
                if (-1 != ret_val)
                {
                    base=mmap(NULL, sizeof(PerformanceCounters),
                              PROT_READ | PROT_WRITE, MAP_SHARED,
                              fd, 0);
                }   // if

                if (MAP_FAILED != base)
                {
                    PerformanceCounters * perf;

                    perf=(PerformanceCounters *)base;
                    if (!perf->VersionTest())
                        perf->Init();

                    gPerfCounters=perf;
                    good=true;
                }   // if
            }   // if

            return(good);

        };  // OpenSharedMemFile
#endif
};  // struct PerformanceCounters


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_
