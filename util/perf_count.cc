// -------------------------------------------------------------------
//
// perf_count.cc:  performance counters LevelDB (http://code.google.com/p/leveldb/)
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

#include <limits.h>
#include <stdio.h>

#ifndef STORAGE_LEVELDB_INCLUDE_PERF_COUNT_H_
#include "leveldb/perf_count.h"
#endif

#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb
{

    SstCounters::SstCounters()
        : m_IsReadOnly(false),
          m_Version(eSstCountVersion),
          m_CounterSize(eSstCountEnumSize)
    {
        memset(m_Counter, 0, sizeof(m_Counter));

        m_Counter[eSstCountKeySmallest]=ULLONG_MAX;
        m_Counter[eSstCountValueSmallest]=ULLONG_MAX;

        return;

    };  // SstCounters::SstCounters


    void
    SstCounters::EncodeTo(
        std::string & Dst) const
    {
        unsigned loop;

        PutVarint32(&Dst, m_Version);
        PutVarint32(&Dst, m_CounterSize);

        for(loop=0; loop<eSstCountEnumSize; ++loop)
            PutVarint64(&Dst, m_Counter[loop]);
    }   // SstCounters::EncodeTo


    Status
    SstCounters::DecodeFrom(
        const Slice& src)
    {
        Status ret_status;
        Slice cursor;
        bool good;
        int loop;

        cursor=src;
        m_IsReadOnly=true;
        good=GetVarint32(&cursor, &m_Version);
        good=good && (m_Version<=eSstCountVersion);

        // all lesser number of stats to be read
        good=good && GetVarint32(&cursor, &m_CounterSize);
        if (good && eSstCountEnumSize < m_CounterSize)
            m_CounterSize=eSstCountEnumSize;

        for (loop=0; good && loop<eSstCountEnumSize; ++loop)
        {
            good=GetVarint64(&cursor, &m_Counter[loop]);
        }   // for

        // if (!good) change ret_status to bad

        return(ret_status);

    }   // SstCounters::DecodeFrom


    uint64_t
    SstCounters::Inc(
        unsigned Index)
    {
        uint64_t ret_val;

        ret_val=0;
        if (!m_IsReadOnly && Index<m_CounterSize)
        {
            ++m_Counter[Index];
            ret_val=m_Counter[Index];
        }   // if

        return(ret_val);
    }   // SstCounters::Inc


    uint64_t
    SstCounters::Add(
        unsigned Index,
        uint64_t Amount)
    {
        uint64_t ret_val;

        ret_val=0;
        if (!m_IsReadOnly && Index<m_CounterSize)
        {
            m_Counter[Index]+=Amount;
            ret_val=m_Counter[Index];
        }   // if

        return(ret_val);
    }   // SstCounters::Add


    uint64_t
    SstCounters::Value(
        unsigned Index)
    {
        uint64_t ret_val;

        ret_val=0;
        if (Index<m_CounterSize)
        {
            ret_val=m_Counter[Index];
        }   // if

        return(ret_val);
    }   // SstCounters::Value


    void
    SstCounters::Set(
        unsigned Index,
        uint64_t Value)
    {
        if (Index<m_CounterSize)
        {
            m_Counter[Index]=Value;
        }   // if

        return;
    }   // SstCounters::Set


    void
    SstCounters::Dump()
    {
        unsigned loop;

        printf("SstCounters:\n");
        printf("   m_IsReadOnly: %u\n", m_IsReadOnly);
        printf("      m_Version: %u\n", m_Version);
        printf("  m_CounterSize: %u\n", m_CounterSize);
        for (loop=0; loop<m_CounterSize; ++loop)
            printf("    Counter[%2u]: %llu\n", loop, m_Counter[loop]);

        return;

    }   // SstCounters::Dump

    void
    PerformanceCounters::Init()
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
    };  // PerformanceCounters::Init


    void
    PerformanceCounters::Dump()
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


}  // namespace leveldb
