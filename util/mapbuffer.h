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

#include <sys/mman.h>

#include <leveldb/env.h>
#include <leveldb/status.h>
#include <port/port.h>

namespace leveldb
{

class RiakBufferFile;

class RiakWriteBuffer
{
public:

protected:

    volatile int m_RefCount;   //!< number of active users of this map, delete on zero
    RiakBufferFile & m_File;   //!< parent file object

    // mapping parameters
    void * m_Base;             //!< memory map location
    off_t m_Offset;            //!< offset into m_File of m_Base
    size_t m_Size;             //!< byte count of the mapping

    port::Mutex m_MapWait;     //!< first assign actually performs mapping, others wait

private:


public:
    RiakWriteBuffer(RiakBufferFile & FileParent, off_t Offset, size_t Size);

    virtual ~RiakWriteBuffer();  // put unmap within file object

    // method used to actually copy data into place (first thread also maps file)
    Status AssignData(const void * Data, size_t DataSize, off_t AssignedOffset);

    // update m_RefCount
    void IncRef();

    // update m_RefCount, potentially delete object (and unmap)
    void DecRef();

    // WARNING:  can return NULL
    const void * get(off_t Offset) const;

    bool ok() const;

    bool FitsMapping(off_t Offset, size_t Size) const
    {return(m_Offset<=Offset && Offset+Size<=m_Offset+m_Size);};

    // send parameters back to parent for msync
    void Sync(); // const ... breaks volatile

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
    bool ok() const {return(NULL!=m_Buffer && m_Buffer->ok());};

    void reset(RiakWriteBuffer & Buffer, off_t Base, size_t Size);

    // is space still available for writing
    bool writable() const {return(0!=m_CurSize);};

    Status assign(const void * Data, size_t DataSize);
    Status append(const void * Data, size_t DataSize);

private:

};  // class RiakBufferPtr


class RiakBufferFile : public WritableFile
{
protected:
    int m_FileDesc;                //!< file desc from open call, or -1
    int m_Advise;                  //!< posix_fadvise value to use
    volatile off_t m_FileSize;     //!< ftruncate size of file
    volatile off_t m_NextOffset;   //!< size used, next file position, etc.
    size_t m_BufferSize;           //!< user supplied buffer size * 1.10
    size_t m_PageSize;             //!< page size/boundry for mapping

    volatile RiakWriteBuffer * m_CurBuffer; //!< buffer to receive next allocation

    Status m_FileOk;               //!< global status for file
    port::Mutex m_Mutex;           //!< protects m_NextOffset/m_CurBuffer evaluation/change
    std::string m_Filename;        //!< save file name for potential error reporting

public:
    RiakBufferFile();
    virtual ~RiakBufferFile();

    virtual Status Open(const std::string & Filename, bool AdviseKeep,
                        size_t WriteBufferSize, size_t PageSize);

    virtual Status Allocate(size_t DataSize, RiakBufferPtr & OutPtr);

    virtual bool SupportsBuilder2() const {return(true);};

    virtual Status Append(const Slice& data);

    virtual Status Close();
    virtual Status Flush();
    virtual Status Sync();

    // methods used by RiakWriteBuffer
    void UnMap(void * Base, off_t Offset, size_t Size);

    Status Map(off_t Offset, size_t Size, void ** Base);

    void MSync(void * Base, size_t Size);

    // global test of ready and working
    bool ok() const {return(m_FileOk.ok() && -1!=m_FileDesc);};

    Status StatusObj() const {return(m_FileOk);};

private:
    // No copying allowed
    RiakBufferFile(const RiakBufferFile&);
    RiakBufferFile & operator=(const RiakBufferFile&);

protected:

private:

};  // class RiakBufferFile


};  // namespace leveldb
