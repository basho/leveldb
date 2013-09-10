// -------------------------------------------------------------------
//
// flexcache.h
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
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

#include "util/cache2.h"


namespace leveldb 
{

/**
 * FlexCache tunes file cache versus block cache versus number
 *  of open databases
 */

class FlexCache
{
public:
    uint64_t GetDBCacheCapacity(bool IsInternalDB);

    void SetTotalMemory(uint64_t Total);

    uint64_t GetTotalMemory() const {return(m_TotalMemory);};


protected:

    uint64_t m_TotalMemory; //!< complete memory assigned to all FlexCache clients


};  // class FlexCache


extern FlexCache gFlexCache;

}  // namespace leveldb
