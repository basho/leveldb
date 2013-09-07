// -------------------------------------------------------------------
//
// db_list.cc
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

#include "util/db_list.h"

namespace leveldb {

// using singleton model from comparator.cc
static port::OnceType once = LEVELDB_ONCE_INIT;
static DBListImpl * dblist=NULL;

static void InitModule()
{
    dblist=new DbListImpl;

}   // InitModule


DBListImpl * DBList()
{
    port::InitOnce(&once, InitModule);
    return(dblist);

}   // DBList


void
DBListShutdown()
{
    // retrieve point to handle any initialization/shutdown races
    DBList();
    delete dblist;

    return;

}   // DBListShutdown


}  // namespace leveldb
