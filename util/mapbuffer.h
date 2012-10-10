/**
 * @file mapbuffer.h
 * @author matthewv
 * @date October 7, 2012
 * @date Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 *
 * @brief Declarations for overlapping writes to memmapped files
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <sys/mmap>

#include <leveldb/status.h>


namespace leveldb
{

class RiakWriteBuffer
{
public:

protected:

    volatile int m_RefCount;       //!< number of active users of this map, delete on zero
    RiakBufferFile & m_File;   //!< parent file object

    // mapping parameters
    volatile void * m_Base;    //!< memory map location
    off_t m_Offset;            //!< offset into m_File of m_Base
    size_t m_Size;             //!< byte count of the mapping

    Mutex m_MapWait;           //!< first assign actually performs mapping, others wait

private:


public:
    explicit RiakWriteBuffer(RiakBufferFile & FileParent);

    virtual ~RiakWriteBuffer();  // put unmap within file object

    // method used to actually copy data into place (first thread also maps file)
    Status AssignData(const void * Data, size_t DataSize, off_t AssignedOffset);

    // update m_RefCount
    void IncRef();

    // update m_RefCount, potentially delete object (and unmap)
    void DecRef();

    // WARNING:  can return NULL
    const void * get(off_t Offset) const;

private:
    RiakWriteBuffer();                                               //!< deny default constructor
    RiakWriteBuffer(const RiakWriteBuffer & rhs);                    //!< deny copy
    RiakWriteBuffer & operator=(const RiakWriteBuffer & rhs);  //!< deny assign

};  // class RiakWriteBuffer


class RiakBufferPtr
{
public:

protected:
    RiakWriteBuffer * m_Buffer;

    off_t m_BaseOffset;       //!< offset assign to pointer
    size_t m_BaseSize;        //!< size allocated to pointer

    // no protection against multiple copies of pointer trying to overwrite
    off_t m_CurOffset;        //!< current write position within assignment
    size_t m_CurSize;         //!< remaining size within assignment


public:
    RiakBufferPtr();
    RiakBufferPtr(RiakWriteBuffer & Buffer, off_t PointerBase, size_t PointerSize);
    RiakBufferPtr(const RiakBufferPtr & rhs);

    virtual ~RiakBufferPtr();

    RiakBufferPtr & operator=(const RiakBufferPtr & rhs);

    //
    // stl like methods
    //
    const void * get() const {return(NULL!=m_Buffer ? m_Buffer->get(m_BaseOffset) : NULL);};
    // ?? void release()?
    bool ok() const {return(NULL!=m_Buffer);};

    // is space still available for writing
    bool writable() const {return(0!=m_CurSize);};

    Status assign(void * Data, size_t DataSize);
    Status append(void * Data, size_t DataSize);

private:

};  // class RiakBufferPtr

#if 0
// map   RiakWriteBuffer
/// file object
/// ref count
//// if ref count goes to zero AND file next offset beyond range, delete
////   on delete tell file object to remove from list
//// OR file object holds one reference while in range, then releases
/// creation mutex, all wait until mapping complete, first caller to 
///  assign/append performs mapping ... allows mappings to be threaded on large blocks
/// memcpy operation that verifies map offset and size
/// mapping offset, size, pointer
/// create UnMap routine in file object, call it on delete / background munmap

// pointer to a map  RiakWritePtr (derived from WritePtr)
/// pointer to object
/// offset in object or total space
/// CopyFirst, CopyNext (assign / append) ... calls object and maybe blocks
/// get() ... fails / asserts / throws if CopyTo not complete,
///           OR gives const pointer where next assign will write
/// ok(), writable() until full, readable() after full
/// release() ... !ok, !writable, !readable

// container RiakBufferFile
/// pointer to current map, ref counted ptr
/// rw lock for map tests
//// obtain read lock
//// validate global status ... assert & return if closed (or not open)
//// assert a mapping exists
//// inc reference lock on current map
//// atomic offset / allocate
//// allocation within current map
//// yes, done ... create and return WritePtr
//// no, release reference, then read lock
//// get write lock,
//// reference lock, retest ... if now good, release write lock & return ptr
////  otherwise release object hold on current map, create new map starting
////  at this caller's point for MIN(caller's size, buffer standard size)
////  MUST make FTRUNCATE call here, not thread delayed (at map object)
/// mutex close or abort? ... old 
/// atomic offset counter
/// is this the file object ... yes
/// fadvise param
/// close sync or async ... sync for now.  ftruncate to current offset
//// use write lock, change global status and shutdown
//// mutex on Close to allow clean unmaps? Or assume something else synchronizes
////  writers to closer ... assert if current map has pointers outstanding, besides file's ptr?
/// Should open create first mapping? only loss is if first object greater than entire mapping.
////  yes, establish assumption one mapping always exists
////  a background worker could be launched to seed new map ... cost/benefit? use zero size mapping?
////  this would anticipate ftruncate only in cases where objects exactly fill current map. 
////   put in comment, but do not code.  a testing bitch
/// Open / Close ... allow stack object creation
/// container level status: m_Status



/// WritableFile needs "allocate pointer"
///  WritePtr allocate(size_t)
///  Fail Write() interface

// NewWritableFile AND NewRiakWritableFile static ???
///  or choose based upon WriteBufferSize ... zero is old PosixMmapFile
///     call NewWritableFileOld (not virtual)
/// hack into existing PosixEnv ... but keep stuff in other files.


virtual Status NewWritableFile(  // only wraps "new" and Open call (delete obj if open fails)
    const std::string &fname, 
    WritableFile **result, 
    bool AdviseKeep=false,
    const size_t WriteBufferSize=0); // use write buffer size * 1.2


#endif

};  // namespace leveldb
