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

#include <errno.h>
#include <features.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include "port/port.h"
#include "util/mapbuffer.h"
#include "util/env_riak.h"

#if _XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L
#define HAVE_FADVISE
#endif

namespace leveldb
{
/**
 * Initialize object
 * @date 10/09/12 Created
 * @author matthewv
 */
RiakWriteBuffer::RiakWriteBuffer(
    RiakBufferFile & ParentFile,              //!< underlying file object
    off_t Offset,                             //!< starting offset of this mapping
    size_t Size)                              //!< size of mapping
    : m_RefCount(1), m_File(ParentFile),
      m_Base(NULL), m_Offset(Offset), m_Size(Size)
{
    // m_RefCount starts at one.  This means caller of "new" implicitly
    //  starts with a reference OR buffer object can be on stack and will
    //  NOT self delete when all pointers go away.

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
        m_File.UnMap(m_Base, m_Offset, m_Size);
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

    if (m_File.ok())
    {
        {
            port::MutexLock lock(m_MapWait);

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
        ret_stat=m_File.StatusObj();
    }   // else

    return(ret_stat);

}   // RiakWriteBuffer::AssignData


/**
 * Add one to our count of pointer references
 * @date 10/10/12 Created
 * @author matthewv
 */
void
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
void
RiakWriteBuffer::DecRef()
{
    if (0<m_RefCount)
    {
        int count;

        assert(0<m_RefCount);

#ifdef OS_SOLARIS
        count=atomic_sub_int(&m_RefCount, 1);
#else
        count=__sync_sub_and_fetch(&m_RefCount, 1);
#endif

        if (0==count && NULL!=m_Base)
        {
            m_File.UnMap(m_Base, m_Offset, m_Size);
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
    if (NULL!=m_Base)
    {
        assert(m_Offset<=Offset && Offset<m_Offset+m_Size);
        if (m_Offset<=Offset && Offset<m_Offset+m_Size)
        {
            off_t map_offset;

            map_offset=Offset-m_Offset;
            ret_ptr=(char *)m_Base + map_offset;
        }   // if
    }   // if

    return(ret_ptr);

}   // RiakWriteBuffer::get


/**
 * Test if object is in good condition
 * @date 10/12/12 Created
 * @author matthewv
 * @returns true if object is usable
 */
bool
RiakWriteBuffer::ok() const
{
    bool ret_flag;

    ret_flag=(m_File.ok() && 0!=m_RefCount);

    assert(ret_flag);

    return(ret_flag);

}   // RiakWriteBuffer::ok


/**
 * Pass mapping parameters to file object for sync
 * @date 10/12/12 Created
 * @author matthewv
 */
void
RiakWriteBuffer::Sync() // const
{
    if (NULL!=m_Base)
        m_File.MSync(m_Base, m_Size);
    else
        assert(false);

    return;

}   // RiakWriteBuffer::ok



/**
 * Default constructor
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferPtr::RiakBufferPtr()
    : m_Buffer(NULL), m_BaseOffset(0), m_BaseSize(0),
      m_CurOffset(0), m_CurSize(0)
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
      m_CurOffset(0), m_CurSize(0)
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
      m_CurOffset(0), m_CurSize(0)
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
 * Assign parameters to pointer
 * @date 10/10/12 Created
 * @author matthewv
 * @returns Status error (either file ioerr or param error)
 */
void
RiakBufferPtr::reset(
    RiakWriteBuffer & Buffer,     //!< buffer containing assigned mapping
    off_t Offset,                 //!< file location of offset
    size_t Size)                  //!< size of allocation
{
    bool same;

    same=(&Buffer==m_Buffer && Offset==m_BaseOffset);

    // release current buffer if assigned and not same as copy
    if (NULL!=m_Buffer && !same)
        m_Buffer->DecRef();

    m_Buffer=&Buffer;
    m_BaseOffset=Offset;
    m_BaseSize=Size;
    m_CurOffset=0;
    m_CurSize=0;

    if (NULL!=m_Buffer && !same)
        m_Buffer->IncRef();

    return;

}   // RiakBufferPtr::operator=
/**
 * Copy data to beginning of allocated space
 * @date 10/10/12 Created
 * @author matthewv
 * @returns Status error (either file ioerr or param error)
 */
Status
RiakBufferPtr::assign(
    const void * Data,
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
    const void * Data,
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


/**
 * Default constructor
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferFile::RiakBufferFile()
    : m_FileDesc(-1), m_Advise(0), m_FileSize(0), m_NextOffset(0),
      m_BufferSize(0), m_CurBuffer(NULL)
{
    return;
}   // RiakBufferFile::RiakBufferFile


/**
 * Destructor, release resources
 * @date 10/10/12 Created
 * @author matthewv
 */
RiakBufferFile::~RiakBufferFile()
{
    Close();

    return;

}   // RiakBufferFile::~RiakBufferFile


/**
 * Open file
 * @date 10/10/12 Created
 * @author matthewv
 */
Status
RiakBufferFile::Open(
    const std::string & Filename,        //!< full path of file to create
    bool AdviseKeep,                     //!< true for POSIX_FADV_WILLNEED, false _DONTNEED
    size_t WriteBufferSize,              //!< will use this times 1.1 as default map size
    size_t PageSize)                     //!< operating system page size
{
    Status ret_stat;

    // stop reuse / retry
    if (-1==m_FileDesc && m_FileOk.ok() && 0!=WriteBufferSize)
    {
        m_Filename=Filename;
        m_FileDesc=open(Filename.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        if (-1!=m_FileDesc)
        {
#ifdef HAVE_FADVISE
            m_Advise=(AdviseKeep ? POSIX_FADV_WILLNEED : POSIX_FADV_DONTNEED);
#endif
            m_FileSize=0;
            m_NextOffset=0;

            // use 1.1 times given size
            m_BufferSize=WriteBufferSize + WriteBufferSize/10;
            m_PageSize=PageSize;
            m_CurBuffer=NULL;
        }   // if
        else
        {
            ret_stat=Status::IOError(m_Filename, strerror(errno));
            m_FileOk=ret_stat;
        }   // else
    }   // if
    else
    {
        assert(false);
        ret_stat=Status::InvalidArgument("Open: bad params");
    }   // else

    return(ret_stat);

}   // RiakBufferFile::Open


/**
 * Assign region of file to caller
 * @date 10/11/12 Created
 * @author matthewv
 * @returns leveldb Status object
 */
Status
RiakBufferFile::Allocate(
    size_t DataSize,
    RiakBufferPtr & OutPtr)
{
    RiakBufferPtr ret_ptr;
    Status ret_stat;

    // This could be done slightly better with a RWLock,
    //  but keeping it simple for now
    if (ok())
    {
        port::MutexLock lock(m_Mutex);
        off_t assigned_off;
        bool done;

        done=false;
        assigned_off=m_NextOffset;
        m_NextOffset+=DataSize;

        // is current mapping sufficient
        if (NULL!=m_CurBuffer)
        {
            RiakWriteBuffer * buf;

            // cast away "volatile" ... we hold lock
            buf=(RiakWriteBuffer *)m_CurBuffer;

            if (!buf->FitsMapping(assigned_off, DataSize))
            {
                buf->DecRef();
                m_CurBuffer=NULL;
            }   // if
        }   // if

        // is a new mapping required
        if (NULL==m_CurBuffer)
        {
            off_t map_offset;
            size_t map_size;
            int ret_val;

            if (DataSize < m_BufferSize )
                map_size=m_BufferSize;
            else
                map_size=DataSize;

            map_offset=(assigned_off / m_PageSize) * m_PageSize;
            map_size=((map_size / m_PageSize) +1) * m_PageSize;

            m_FileSize+=map_size;
            ret_val=ftruncate(m_FileDesc, m_FileSize);
            if (0==ret_val)
            {
                // need try/catch logic for memory failures
                m_CurBuffer=new RiakWriteBuffer(*this, map_offset, map_size);
            }   // if
            else
            {
                // leave m_CurBuffer as NULL and set into ret_ptr
                m_FileOk=Status::IOError(m_Filename, strerror(errno));
            }   // else
        }   // if

        // cast away "volatile" ... we hold lock
        ret_ptr.reset((RiakWriteBuffer &)*m_CurBuffer, assigned_off, DataSize);
    }   // MutexLock

    OutPtr=ret_ptr;
    return(ret_stat);

}   // RiakBufferFile::Allocate


/**
 * Wrapper to support old style Append virtual
 * @date 10/11/12 Created
 * @author matthewv
 */
Status
RiakBufferFile::Append(
    const Slice & Data)
{
    Status ret_stat;
    RiakBufferPtr ptr;

    ret_stat=Allocate(Data.size(), ptr);

    if (ret_stat.ok() && ptr.ok())
        ptr.assign(Data.data(), Data.size());
    else if (ret_stat.ok())
        ret_stat=Status::InvalidArgument(Slice("RiakBufferFile::Append() failed."));

    return(ret_stat);

}   // RiakBufferFile::Append


/**
 * Perform file close, assumes caller has shutdown threads that might hold RiakBufferPtr()
 * @date 10/11/12 Created
 * @author matthewv
 * @returns leveldb Status object
 */
Status
RiakBufferFile::Close()
{
    Status ret_stat;
    port::MutexLock lock(m_Mutex);

    if (NULL!=m_CurBuffer)
    {
        // hope all pointers were released ...
        ((RiakWriteBuffer *)m_CurBuffer)->DecRef();
//        delete m_CurBuffer;
        m_CurBuffer=NULL;
    }   // if

    if (-1!=m_FileDesc)
    {
        ftruncate(m_FileDesc, m_NextOffset);
        close(m_FileDesc);
        m_FileDesc=-1;
    }   // if

    return(ret_stat);

}   // RiakBufferFile::Close


/**
 * Perform user request flush of data to disk
 * @date 10/11/12 Created
 * @author matthewv
 * @returns leveldb Status object
 */
Status
RiakBufferFile::Flush()
{
    Status ret_stat;

    // noop for now (same as previous code)

    return(ret_stat);

}   // RiakBufferFile::Flush


/**
 * Perform user request sync of data to disk
 * @date 10/11/12 Created
 * @author matthewv
 * @returns leveldb Status object
 */
Status
RiakBufferFile::Sync()
{
    Status ret_stat;
#if 1
    if (ok())
    {
        int ret_val;
        // async nature implies that we do not know
        //  that every file buffer was sync'd
        do
        {
            ret_val=fdatasync(m_FileDesc);
        } while(EINTR==ret_val);

        if (0!=ret_val)
        {
            ret_stat=Status::IOError(m_Filename, strerror(errno));
            m_FileOk=ret_stat;
        }   // if

        port::MutexLock lock(m_Mutex);
        if (NULL!=m_CurBuffer)
        {
            RiakWriteBuffer *buf;

            buf=(RiakWriteBuffer *)m_CurBuffer;
            buf->Sync();
        }   // if
    }   // if
    else
    {
        assert(false);
        if (m_FileOk.ok())
            m_FileOk=Status::InvalidArgument(Slice("RiakBufferFile::Sync() on invalid file"));
        ret_stat=m_FileOk;
    }   // else;
#endif
    return(ret_stat);

}   // RiakBufferFile::Sync


/**
 * Routine used by RiakWriteBuffer to request a new mapping
 * @date 10/11/12 Created
 * @author matthewv
 * @returns leveldb Status object
 */
Status
RiakBufferFile::Map(
    off_t Offset,
    size_t Size,
    void ** Base)
{
    Status ret_stat;

    if (ok())
    {
        *Base=mmap(NULL, Size, PROT_READ | PROT_WRITE, MAP_SHARED, m_FileDesc, Offset);
        if (MAP_FAILED==*Base)
        {
            assert(false);
            ret_stat=Status::IOError(m_Filename, strerror(errno));
            m_FileOk=ret_stat;
            *Base=NULL;
        }   // if
    }   // if
    else
    {
        assert(false);
        if (m_FileOk.ok())
            m_FileOk=Status::InvalidArgument(Slice("RiakBufferFile::Sync() on invalid file"));
        ret_stat=m_FileOk;
    }   // else;

    return(ret_stat);

}   // RiakBufferFile::Map


/**
 * Routine used by RiakWriteBuffer to release a mapping
 * @date 10/11/12 Created
 * @author matthewv
 */
void
RiakBufferFile::UnMap(
    void * Base,
    off_t Offset,
    size_t Size)
{
    if (ok())
    {
#if defined(HAVE_FADVISE)
        posix_fadvise(m_FileDesc, Offset, Size, m_Advise);
#endif

        BGCloseInfo * ptr=new BGCloseInfo(-1, Base, 0, Size, 0);
        Env::Default()->Schedule(&BGFileUnmapper, ptr, 4);

#if 0
        if (0!=munmap(Base, Size))
        {
            assert(false);
            m_FileOk=Status::IOError(m_Filename, strerror(errno));
        }   // if
#endif
    }   // if
    else
    {
        assert(false);
        if (m_FileOk.ok())
            m_FileOk=Status::InvalidArgument(Slice("RiakBufferFile::UnMap() on invalid file"));
    }   // else;

    return;

}   // RiakBufferFile::UnMap


/**
 * Perform sync requested by buffer
 * @date 10/12/12 Created
 * @author matthewv
 */
void
RiakBufferFile::MSync(
    void * Base,
    size_t Size)
{
#if 0
    if (ok())
    {
        // async nature implies that we do not know
        //  that every file buffer was sync'd
        if (0!=msync(Base, Size, MS_ASYNC))
        {
            m_FileOk=Status::IOError(m_Filename, strerror(errno));
        }   // if
    }   // if
    else
    {
        assert(false);
        if (m_FileOk.ok())
            m_FileOk=Status::InvalidArgument(Slice("RiakBufferFile::MSync() on invalid file"));
    }   // else;
#endif
    return;

}   // RiakBufferFile::MSync


};  // namespace leveldb
