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


namespace leveldb {

struct





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

            printf(" m_ROFileOpen: %lu\n", m_ROFileOpen);
            printf(" m_ROFileClose: %lu\n", m_ROFileClose);
            printf(" m_ROFileUnmap: %lu\n", m_ROFileUnmap);

            printf(" m_ApiOpen: %lu\n", m_ApiOpen);
            printf(" m_ApiGet: %lu\n", m_ApiGet);
            printf(" m_ApiWrite: %lu\n", m_ApiWrite);

            printf(" m_WriteSleep: %lu\n", m_WriteSleep);
            printf(" m_WriteWaitImm: %lu\n", m_WriteWaitImm);
            printf(" m_WriteWaitLevel0: %lu\n", m_WriteWaitLevel0);
            printf(" m_WriteNewMem: %lu\n", m_WriteNewMem);
            printf(" m_WriteError: %lu\n", m_WriteError);
            printf(" m_WriteNoWait: %lu\n", m_WriteNoWait);

            printf(" m_GetMem: %lu\n", m_GetMem);
            printf(" m_GetImm: %lu\n", m_GetImm);
            printf(" m_GetVersion: %lu\n", m_GetVersion);

            printf(" m_SearchLevel[0]: %lu\n", m_SearchLevel[0]);
            printf(" m_SearchLevel[1]: %lu\n", m_SearchLevel[1]);
            printf(" m_SearchLevel[2]: %lu\n", m_SearchLevel[2]);
            printf(" m_SearchLevel[3]: %lu\n", m_SearchLevel[3]);
            printf(" m_SearchLevel[4]: %lu\n", m_SearchLevel[4]);
            printf(" m_SearchLevel[5]: %lu\n", m_SearchLevel[5]);
            printf(" m_SearchLevel[6]: %lu\n", m_SearchLevel[6]);

            printf(" m_TableCached: %lu\n", m_TableCached);
            printf(" m_TableOpened: %lu\n", m_TableOpened);
            printf(" m_TableGet: %lu\n", m_TableGet);

            printf(" m_BGCloseUnmap: %lu\n", m_BGCloseUnmap);
            printf(" m_BGCompactImm: %lu\n", m_BGCompactImm);
            printf(" m_BGNormal: %lu\n", m_BGNormal);

            printf(" m_BlockFiltered: %lu\n", m_BlockFiltered);
            printf(" m_BlockFilterFalse: %lu\n", m_BlockFilterFalse);
            printf(" m_BlockCached: %lu\n", m_BlockCached);
            printf(" m_BlockRead: %lu\n", m_BlockRead);
            printf(" m_BlockFilterRead: %lu\n", m_BlockFilterRead);
            printf(" m_BlockValidGet: %lu\n", m_BlockValidGet);

            printf(" m_Debug[0]: %lu\n", m_Debug[0]);
            printf(" m_Debug[1]: %lu\n", m_Debug[1]);
            printf(" m_Debug[2]: %lu\n", m_Debug[2]);
            printf(" m_Debug[3]: %lu\n", m_Debug[3]);
            printf(" m_Debug[4]: %lu\n", m_Debug[4]);
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
