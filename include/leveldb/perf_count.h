// Copyright (c) 2012 Basho. All rights reserved.



#ifndef STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_
#define STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_


namespace leveldb {

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

    volatile uint64_t m_BGCloseUnmap;   //!< PosixEnv::BGThreaed started Unmap/Close job
    volatile uint64_t m_BGCompactImm;   //!< PosixEnv::BGThreaed started compaction of Imm or Level0
    volatile uint64_t m_BGNormal;       //!< PosixEnv::BGThreaed started normal compaction job

    volatile uint64_t m_BlockFiltered;  //!< Table::BlockReader search stopped due to filter
    volatile uint64_t m_BlockCached;    //!< Table::BlockReader found block in cache
    volatile uint64_t m_BlockRead;      //!< Table::BlockReader read block from disk
    volatile uint64_t m_BlockFilterRead;//!< Table::ReadMeta filter loaded from file

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

            m_BGCloseUnmap=0;
            m_BGCompactImm=0;
            m_BGNormal=0;

            m_BlockFiltered=0;
            m_BlockCached=0;
            m_BlockRead=0;
            m_BlockFilterRead=0;
        };
};  // struct PerformanceCounters


extern PerformanceCounters * gPerfCounters;

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_
