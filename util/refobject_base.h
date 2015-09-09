// -------------------------------------------------------------------
//
// refobject_base.h
//
// Copyright (c) 2015 Basho Technologies, Inc. All Rights Reserved.
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

// -------------------------------------------------------------------
//  Base class for reference-counted types; refactored from
//  eleveldb/c_src/refobjects.h and leveldb/util/thread_tasks.h
// -------------------------------------------------------------------

#ifndef LEVELDB_INCLUDE_REFOBJECT_BASE_H_
#define LEVELDB_INCLUDE_REFOBJECT_BASE_H_

#include <stdint.h>

#include "leveldb/atomics.h"

namespace leveldb {

/**
 * Base class for reference-counted types
 *
 * A user of a reference-counted object makes the reference explicit by
 * calling the RefInc() method, which increments the internal reference
 * counter in a thread safe manner. When the user of the object is done
 * with the object, it releases the reference by calling the RefDec()
 * method, which decrements the internal counter in a thread safe manner.
 * When the reference counter reaches 0, the RefDec() method deletes
 * the current object by executing a "delete this" statement.
 *
 * Note that the because RefDec() executes "delete this" when the reference
 * count reaches 0, the reference-counted object must be allocated on the
 * heap.
 */
class RefObjectBase
{
 protected:
    volatile uint32_t m_RefCount;

 public:
    RefObjectBase() : m_RefCount(0) {}
    virtual ~RefObjectBase() {}

    uint32_t RefInc() {return(inc_and_fetch(&m_RefCount));}

    uint32_t RefDec()
    {
        uint32_t current_refs;

        current_refs=dec_and_fetch(&m_RefCount);
        if (0==current_refs) {
            delete this;
        }

        return(current_refs);
    }

 private:
    // hide the copy ctor and assignment operator (not implemented)
    RefObjectBase(const RefObjectBase&);
    RefObjectBase& operator=(const RefObjectBase&);
};

} // namespace leveldb

#endif  // LEVELDB_INCLUDE_REFOBJECT_BASE_H_
