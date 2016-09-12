// -------------------------------------------------------------------
//
// ldb_table.h
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

#include "rewrite_state.h"

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/table.h"

// wrapper class for opening / closing existing leveldb tables
class LDbTable
{
public:
    LDbTable(leveldb::Options &, const std::string &);
    virtual ~LDbTable();

    bool Ok() const {return(m_IsOpen);};
    leveldb::Iterator * NewIterator();

    const leveldb::Status & GetStatus() const {return(m_LastStatus);};
    const char * GetFileName() const {return(m_FileName.c_str());};

    uint64_t GetSstCounter(unsigned Idx) const
        {return(m_IsOpen ? m_TablePtr->GetSstCounters().Value(Idx) : 0);};

    leveldb::CompressionType CompressionType()
    {return(m_IsOpen ? m_TablePtr->GetCompressionType() : leveldb::kNoCompression);};

    bool HasExpiry();

protected:
    leveldb::Options & m_Options;
    const std::string m_FileName;
    leveldb::RandomAccessFile * m_FilePtr;
    leveldb::Table * m_TablePtr;
    uint64_t m_FileSize;
    leveldb::Status m_LastStatus;

    bool m_IsOpen;

    void Reset();

private:
    // disable these
    LDbTable();
    LDbTable(const LDbTable &);
    const LDbTable operator=(const LDbTable&);
};  // LDbTable
