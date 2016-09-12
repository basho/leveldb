// -------------------------------------------------------------------
//
// ldb_table.cc
//
// Copyright (c) 2015-2016 Basho Technologies, Inc. All Rights Reserved.
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

#include "ldb_table.h"

LDbTable::LDbTable(
    leveldb::Options & Options,
    const std::string & FileName)
    : m_Options(Options), m_FileName(FileName),
      m_FilePtr(NULL), m_TablePtr(NULL), m_FileSize(0), m_IsOpen(false)
{
    m_LastStatus=m_Options.env->GetFileSize(m_FileName, &m_FileSize);

    if (m_LastStatus.ok())
        {m_LastStatus=m_Options.env->NewRandomAccessFile(m_FileName, &m_FilePtr);}

    if (m_LastStatus.ok())
    {
        m_LastStatus=leveldb::Table::Open(m_Options, m_FilePtr, m_FileSize, &m_TablePtr);

        // use fadvise to start file pre-read
        m_FilePtr->SetForCompaction(m_FileSize);
    }   // if

    m_IsOpen=m_LastStatus.ok();

    if (!m_IsOpen)
    {
        // some people would throw() at this point, but not me
        Reset();
    }   // if

    return;

}   // LDbTable::LDbTable


LDbTable::~LDbTable()
{
    Reset();

    return;

}   // LDbTable::~LDbTable


void
LDbTable::Reset()
{
    m_IsOpen=false;
    delete m_TablePtr;
    m_TablePtr=NULL;
    delete m_FilePtr;
    m_FilePtr=NULL;
    m_FileSize=0;

    return;

}   // LDbTable::Reset


leveldb::Iterator *
LDbTable::NewIterator()
{
    leveldb::Iterator * ret_ptr(NULL);

    if (m_IsOpen)
    {
        leveldb::ReadOptions read_options;

        read_options.fill_cache=false;
        ret_ptr=m_TablePtr->NewIterator(read_options);
    }   // if

    return(ret_ptr);

}   // LDbTable::NewIterator


bool
LDbTable::HasExpiry()
{
    bool ret_flag;

    // Expiry2 or Expiry3 will be non-zero if any key in the file contains
    //  expiry information.  Expiry1 could be marked as "has keys with expiry" so ignore
    ret_flag=m_IsOpen
        && (0!=GetSstCounter(leveldb::eSstCountExpiry2) || 0!=GetSstCounter(leveldb::eSstCountExpiry3));

    return(ret_flag);

}   // LDbTable::HasExpiry


