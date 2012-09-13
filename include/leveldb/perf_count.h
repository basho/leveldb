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

    void Init()
        {
            m_StructSize=sizeof(PerformanceCounters);
            m_Version=eVersion;
            m_ROFileOpen=0;
            m_ROFileClose=0;
            m_ROFileUnmap=0;

            m_RWFileOpen=0;
            m_RWFileClose=0;
            m_RWFileUnmap=0;

            m_ApiOpen=0;
            m_ApiGet=0;
            m_ApiWrite=0;

            m_WriteSleep=0;
            m_WriteWaitImm=0;
            m_WriteWaitLevel0=0;
            m_WriteNewMem=0;
            m_WriteError=0;
            m_WriteNoWait=0;

            m_GetMem=0;
            m_GetImm=0;
            m_GetVersion=0;

            m_SearchLevel[0]=0;
            m_SearchLevel[1]=0;
            m_SearchLevel[2]=0;
            m_SearchLevel[3]=0;
            m_SearchLevel[4]=0;
            m_SearchLevel[5]=0;
            m_SearchLevel[6]=0;

            m_TableCached=0;
            m_TableOpened=0;
            m_TableGet=0;

            m_BGCloseUnmap=0;
            m_BGCompactImm=0;
            m_BGNormal=0;

            m_BlockFiltered=0;
            m_BlockFilterFalse=0;
            m_BlockCached=0;
            m_BlockRead=0;
            m_BlockFilterRead=0;
            m_BlockValidGet=0;

            m_Debug[0]=0;
            m_Debug[1]=0;
            m_Debug[2]=0;
            m_Debug[3]=0;
            m_Debug[4]=0;
        };  // Init


    void Dump()
        {
            printf(" m_StructSize: %u\n", m_StructSize);
            printf(" m_Version: %u\n", m_Version);

            printf(" m_ROFileOpen: %llu\n", m_ROFileOpen);
            printf(" m_ROFileClose: %llu\n", m_ROFileClose);
            printf(" m_ROFileUnmap: %llu\n", m_ROFileUnmap);

            printf(" m_ApiOpen: %llu\n", m_ApiOpen);
            printf(" m_ApiGet: %llu\n", m_ApiGet);
            printf(" m_ApiWrite: %llu\n", m_ApiWrite);

            printf(" m_WriteSleep: %llu\n", m_WriteSleep);
            printf(" m_WriteWaitImm: %llu\n", m_WriteWaitImm);
            printf(" m_WriteWaitLevel0: %llu\n", m_WriteWaitLevel0);
            printf(" m_WriteNewMem: %llu\n", m_WriteNewMem);
            printf(" m_WriteError: %llu\n", m_WriteError);
            printf(" m_WriteNoWait: %llu\n", m_WriteNoWait);

            printf(" m_GetMem: %llu\n", m_GetMem);
            printf(" m_GetImm: %llu\n", m_GetImm);
            printf(" m_GetVersion: %llu\n", m_GetVersion);

            printf(" m_SearchLevel[0]: %llu\n", m_SearchLevel[0]);
            printf(" m_SearchLevel[1]: %llu\n", m_SearchLevel[1]);
            printf(" m_SearchLevel[2]: %llu\n", m_SearchLevel[2]);
            printf(" m_SearchLevel[3]: %llu\n", m_SearchLevel[3]);
            printf(" m_SearchLevel[4]: %llu\n", m_SearchLevel[4]);
            printf(" m_SearchLevel[5]: %llu\n", m_SearchLevel[5]);
            printf(" m_SearchLevel[6]: %llu\n", m_SearchLevel[6]);

            printf(" m_TableCached: %llu\n", m_TableCached);
            printf(" m_TableOpened: %llu\n", m_TableOpened);
            printf(" m_TableGet: %llu\n", m_TableGet);

            printf(" m_BGCloseUnmap: %llu\n", m_BGCloseUnmap);
            printf(" m_BGCompactImm: %llu\n", m_BGCompactImm);
            printf(" m_BGNormal: %llu\n", m_BGNormal);

            printf(" m_BlockFiltered: %llu\n", m_BlockFiltered);
            printf(" m_BlockFilterFalse: %llu\n", m_BlockFilterFalse);
            printf(" m_BlockCached: %llu\n", m_BlockCached);
            printf(" m_BlockRead: %llu\n", m_BlockRead);
            printf(" m_BlockFilterRead: %llu\n", m_BlockFilterRead);
            printf(" m_BlockValidGet: %llu\n", m_BlockValidGet);

            printf(" m_Debug[0]: %llu\n", m_Debug[0]);
            printf(" m_Debug[1]: %llu\n", m_Debug[1]);
            printf(" m_Debug[2]: %llu\n", m_Debug[2]);
            printf(" m_Debug[3]: %llu\n", m_Debug[3]);
            printf(" m_Debug[4]: %llu\n", m_Debug[4]);
        };  // Dump

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
