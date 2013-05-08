// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.

// A riak addon to original TableBuilder creating parallel compression for multiple blocks

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER2_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER2_H_

#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/block_builder.h"

namespace leveldb {

class BlockBuilder;
class BlockHandle;
class WritableFile;

class TableBuilder2 :public TableBuilder {
private:
    enum
    {
        eTB2Threads=4,    //!< number of compression threads
        eTB2Buffers=256,   //!< number of shared block buffers (was 12)
    };


public:
    TableBuilder2(const Options& options, WritableFile* file, int PriorityLevel);

    // REQUIRES: Either Finish() or Abandon() has been called.
    virtual ~TableBuilder2();

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    virtual void Add(const Slice& key, const Slice& value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: Finish(), Abandon() have not been called
    virtual void Flush();

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    virtual Status Finish();

    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call Finish(), it must call Abandon()
    // before destroying this builder.
    // REQUIRES: Finish(), Abandon() have not been called
    virtual void Abandon();

    // debug tool
    void Dump() const;

protected:
    struct BlockNState
    {
        enum BNState
        {
            eBNStateEmpty=0,    //!< BlockNState object unused
            eBNStateLoading=1,  //!< BlockNState object contains some data
            eBNStateFull=2,     //!< BlockNState object has data, needs compression
            eBNStateCompress=3, //!< BlockNState object in process of compressing
            eBNStateKeyWait=4,  //!< compression complete, but no "next key"
            eBNStateReady=5,    //!< ready for write, but not first on list
            eBNStateWriting=6,  //!< write in progress now
            eBNStateCopying=7   //!< write position known, copying data into map
        };

        volatile BNState m_State;
        CompressionType m_Type;           //!< block type:  compressed or not
        BlockBuilder m_Block;
        std::string m_LastKey;            //!< last key written to block, shortened if m_KeyShortened true
        volatile bool m_KeyShortened;     //!< true when first key of next block seen and applied
        uint32_t m_Crc;                   //!< crc after potential compression
        std::string m_FiltKeys;           //!< keys to put into filter
        std::vector<size_t> m_FiltLengths; //!< size of each key dumped into m_FiltKeys

        BlockNState()
        : m_State(eBNStateEmpty), m_Type(kNoCompression), m_KeyShortened(false), m_Crc(0) {reset();};

        void reset()
        {
            m_State=eBNStateEmpty;
            m_Type=kNoCompression;
            m_Block.Reset();
            m_LastKey.clear();
            m_KeyShortened=false;
            m_Crc=0;
            m_FiltKeys.clear();
            m_FiltLengths.clear();
        }   // reset

        bool empty() const {return(eBNStateEmpty==m_State);}
    };

    volatile bool m_Abort;                        //!< indicate when work should just stop, incomplete
    volatile bool m_Finish;                       //!< indicate no more inbound keys, finish and exit
    port::Mutex m_CvMutex;                        //!< mutex tied to condition variable
    port::CondVar m_CondVar;                      //!<
    pthread_t m_Writers[eTB2Threads];
    BlockNState m_Blocks[eTB2Buffers];
    volatile unsigned m_NextAdd;                          //!< m_Block for Add, changed only by Add thread(s)
    volatile unsigned m_NextWrite;
    int m_PriorityLevel;                          //!< level of output file builder2 is creating

    volatile uint64_t m_TimerReadWait;

    // WorkerThreadEntry calls here with C++ object established
    void WorkerThread();

    // pthread_create entry point, C type function
    static void* WorkerThreadEntry(void* arg)
    {
        reinterpret_cast<TableBuilder2 *>(arg)->WorkerThread();
        return NULL;
    };

public:  // temporarily public to allow perf annotate to comment
    void CompressBlock(BlockNState & State);
    void WriteBlock2(BlockNState & State);

private:
    // No copying allowed
    TableBuilder2(const TableBuilder2&);
    void operator=(const TableBuilder2&);
};  // struct TableBuilder2

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER2_H_
