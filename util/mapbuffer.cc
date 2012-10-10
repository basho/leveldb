/**
 * @file mapbuffer.cc
 * @author matthewv
 * @date October 9, 2012
 * @date Copyright (c) 2012 Basho Technologies, Inc. All Rights Reserved.
 *
 * @brief Implementations for overlapping writes to memmapped files
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


/**
 * Initialize object
 * @date 10/09/12 Created
 * @author matthewv
 */
RiakWriteBuffer::RiakWriteBuffer(
    RiakBufferFile & ParentFile)              //!< underlying file object
    : m_RefCount(0), m_File(ParentFile),
      m_Base(NULL), m_Offset(0), m_Size(0)
{
    return;
}   // RiakWriteBuffer::RiakWriteBuffer


/**
 * Release resources
 * @date 10/09/12 Created
 * @author matthewv
 */
RiakWriteBuffer::~RiakWriteBuffer()
{
    assert(0==m_RefCount);  // otherwise pointers are about to be bad

    if (NULL!=m_Base)
    {
        m_File.Unmap(m_Base, m_Size);
    }   // if

    return;

}   // RiakWriteBuffer::~RiakWriteBuffer


/**
 * Copy data into memory mapping, potentially opening the mapping
 * @date 10/09/12 Created
 * @author matthewv
 */
Status
RiakWriteBuffer::AssignData(
    const void * Data,             //!< source to copy into map
    size_t DataSize,               //!< number of bytes to copy
    off_t AssignedOffset)          //!< offset from beginning of file where data goes
{
    Status ret_stat;

    if (m_File.Status().ok)
    {
        {
            MutexLock lock(m_MapWait);

            // has someone performed the mapping?
            //   first caller does it, other callers wait on mutex until map ready
            if (NULL==m_Base)
            {
                // all actual file operation happen within file object
                ret_stat=m_File.Map(m_Offset, m_Size, &m_Base);
            }   // if
        }   // MutexLock

        assert(true==ret_stat.ok());
        if (ret_stat.ok() && 0<DataSize)
        {
            // only copy if range valid
            if (m_Offset<=AssignedOffset 
                && AssignedOffset+DataSize<=m_Offset+m_Size)
            {
                off_t map_offset;
                char * dst_ptr;

                map_offset=AssignedOffset-m_Offset;
                dst_ptr=(char *)m_Base + map_offset;
                memcpy(dst_ptr, Data, DataSize);
            }   // if
            else
            {
                assert(false);
                ret_stat=Status::InvalidArgument("AssignData: params outside mapping");
            }   // else
        }   // if
    }   // if
    else
    {
        ret_status=m_File.Status();
    }   // else

    return(ret_stat);

}   // RiakWriteBuffer::AssignData


/**
 * Add one to our count of pointer references
 * @date 10/10/12 Created
 * @author matthewv
 */
Status
RiakWriteBuffer::IncRef()
{
#ifdef OS_SOLARIS
    atomic_add_int(&m_RefCount, 1);
#else
    __sync_add_and_fetch(&m_RefCount, 1);
#endif

    return;

}   // RiakWriteBuffer::IncRef


/**
 * Remove one count and maybe delete self
 * @date 10/10/12 Created
 * @author matthewv
 */
Status
RiakWriteBuffer::DecRef()
{
    if (0<m_RefCount && NULL!=m_Base)
    {
        int count;

        assert(0<m_RefCount);

#ifdef OS_SOLARIS
        count=atomic_sub_int(&m_RefCount, 1);
#else
        count=__sync_sub_and_fetch(&m_RefCount, 1);
#endif

        if (0==count)
        {
            m_File.UnMap(m_Base, m_Size);
            m_Base=NULL;
            delete this;
        }   // if
    }   // if

    return;

}   // RiakWriteBuffer::DecRef


/**
 * Return a read pointer into buffer
 * @date 10/10/12 Created
 * @author matthewv
 * @returns NULL or pointer to location in the buffer, no reference lock
 */
const void * 
RiakWriteBuffer::get(
    off_t Offset) const   //!< offset from beginning of file
{
    const void * ret_ptr;
    
    ret_ptr=NULL;

    // just in case someone tries to get a pointer before writing:
    if (NULL==m_Base)
        AssignData(NULL, 0, Offset);

    assert(m_Offset<=Offset && Offset<m_Offset+m_Size);
    if (m_Offset<=Offset && Offset<m_Offset+m_Size)
    {
        off_t map_offset;

        map_offset=Offset-m_Offset;
        ret_ptr=(char *)m_Base + map_offset;
    }   // if

    return(ret_ptr);

}   // RiakWriteBuffer::get


/**
 * Default constructor
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferPtr::RiakBufferPtr()
    : m_Buffer(NULL), m_BaseOffset(0), m_BaseSize(0), 
      m_CurOffset(NULL), m_CurSize(0)
{
    return;
}   // RiakBufferPtr::RiakBufferPtr  (default)


/**
 * Copy constructor
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferPtr::RiakBufferPtr(
    const RiakBufferPtr & Rhs)      //!< existing pointer to copy
    : m_Buffer(NULL), m_BaseOffset(0), m_BaseSize(0), 
      m_CurOffset(NULL), m_CurSize(0)
{
    *this=Rhs;

    return;

}   // RiakBufferPtr::RiakBufferPtr (copy)


/**
 * "new" Constructor with params
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferPtr::RiakBufferPtr(
    RiakWriteBuffer & Buffer,
    off_t PointerBase,
    size_t PointerSize)
    : m_Buffer(&Buffer), m_BaseOffset(PointerBase), m_BaseSize(PointerSize), 
      m_CurOffset(NULL), m_CurSize(0)
{
    m_Buffer->IncRef();

    return;

}   // RiakBufferPtr::RiakBufferPtr (full creation)


/**
 * Release resources
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferPtr::~RiakBufferPtr()
{
    if (NULL!=m_Buffer)
        m_Buffer->DecRef();

    m_Buffer=NULL;
    m_BaseOffset=0;

    return;

}   // RiakBufferPtr::~RiakBufferPtr


/**
 * Assignment/Copy another pointer
 * @date 10/10/12 Created
 * @author matthewv
 * @returns reference to self
 */
RiakBufferPtr &
RiakBufferPtr::operator=(
    const RiakBufferPtr & rhs)
{
    bool same;

    same=(rhs.m_Buffer==m_Buffer && rhs.m_BaseOffset==m_BaseOffset);

    // release current buffer if assigned and not same as copy
    if (NULL!=m_Buffer && !same)
        m_Buffer->DecRef();

    m_Buffer=rhs.m_Buffer;
    m_BaseOffset=rhs.m_BaseOffset;
    m_BaseSize=rhs.m_BaseSize;
    m_CurOffset=rhs.m_CurOffset;
    m_CurSize=rhs.m_CurSize;

    if (NULL!=m_Buffer && !same)
        m_Buffer->IncRef();
    
    return(*this);

}   // RiakBufferPtr::operator=


/**
 * Copy data to beginning of allocated space
 * @date 10/10/12 Created
 * @author matthewv
 * @returns Status error (either file ioerr or param error)
 */
Status
RiakBufferPtr::assign(
    void * Data,
    size_t DataSize)
{
    Status ret_stat;

    if (NULL!=m_Buffer && DataSize<=m_BaseSize 
        && (NULL!=Data || 0==DataSize))
    {
        if (0!=DataSize)
            m_Buffer->AssignData(Data, DataSize, m_BaseOffset);

        m_CurOffset=m_BaseOffset+DataSize;
        m_CurSize=m_BaseSize-DataSize;
        
    }   // if
    else
    {
        ret_stat=Status::InvalidArgument("assign: params outside mapping");
    }   // else

    return(ret_stat);

}   // RiakBufferPtr::assign


/**
 * Copy data after previous data on this pointer
 * @date 10/10/12 Created
 * @author matthewv
 * @returns Status error (either file ioerr or param error)
 */
Status
RiakBufferPtr::append(
    void * Data,
    size_t DataSize)
{
    Status ret_stat;

    if (NULL!=m_Buffer && DataSize<=m_CurSize 
        && (NULL!=Data || 0==DataSize))
    {
        if (0!=DataSize)
            m_Buffer->AssignData(Data, DataSize, m_CurOffset);

        m_CurOffset+=DataSize;
        m_CurSize-=DataSize;
        
    }   // if
    else
    {
        ret_stat=Status::InvalidArgument("append: params outside mapping");
    }   // else

    return(ret_stat);

}   // RiakBufferPtr::append




#if 0
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
