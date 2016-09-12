// -------------------------------------------------------------------
//
// rewrite_state.h
//
// Copyright (c) 2016 Basho Technologies, Inc. All Rights Reserved.
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

#ifndef REWRITE_STATE_H
#define REWRITE_STATE_H

#include <iterator>
#include <string>
#include <stdarg.h>

#include "include/leveldb/options.h"

/**
 * Simple iterator wrapper for command line options
 */
class OptionsIter : public std::iterator<std::bidirectional_iterator_tag, const char *>
{
public:
    OptionsIter() {cursor=NULL;};
    OptionsIter(const char * const * Argv) {cursor=Argv;};
    ~OptionsIter() {};

    const char * operator=(const char ** Argv) {cursor=Argv; return(*cursor);};
    const char * operator*() {return(*cursor);};
    //const char * & operator&() {return(*cursor);};
    const char * operator->() {return(*cursor);};
    const char * operator++() {return(*(++cursor));};
    const char * operator--() {return(*(--cursor));};
    bool operator==(const OptionsIter & rhs)
        {return(cursor==rhs.cursor);};
    bool operator!=(const OptionsIter & rhs)
        {return(cursor!=rhs.cursor);};
    bool operator<(const OptionsIter & rhs)
        {return(cursor<rhs.cursor);};
    void clear() {cursor=NULL;};

protected:
    const char * const * cursor;

};  // class OptionsIter


/**
 * class to accumulate state of command line options
 *  that are passed one at a time.
 */
class RewriteState
{
public:
    RewriteState();
    virtual ~RewriteState() {};

    bool ApplyNext(OptionsIter &Option);

    enum RewriteAction_t
    {
        eDoNothing=0,
        eEnd=1,
        eRewriteFile=2,
        eCompareFiles=3,
        eRewriteDatabase=4,
        eRewritePlatform=5,
        eRewriteTiered=6
    };

    RewriteAction_t GetNextAction() const {return(m_NextAction);};

    void SetFail(const char * Format, ...);
    void Log(const char * Format, ...);

    int GetBlockSize() const {return(m_BlockSize);};

    bool IsGood() const {return(m_Good);};
    bool IsVerbose() const {return(m_Verbose);};
    bool IsForce() const {return(m_ForceRewrite);};
    bool IsTrialRun() const {return(m_TrialRun);};
    bool IsPurgeExpiry() const {return(m_PurgeExpiry);};

    leveldb::CompressionType CompressionType() const {return(m_Compression);};
    bool KeepGoing() const {return(m_KeepGoing);};

    const std::string & GetString(int Index) const {return(m_String[Index]);};

protected:
    bool m_Good;
    RewriteAction_t m_NextAction;
    int m_BlockSize;
    int m_FirstSlowTier;
    leveldb::CompressionType m_Compression;
    bool m_IsRewrite;
    bool m_PurgeExpiry;
    bool m_ManifestOnly;
    bool m_Verbose;
    bool m_TrialRun;
    bool m_ForceRewrite;
    bool m_KeepGoing;

    std::string m_String[2];

};  // class RewriteState

#endif // ifndef REWRITE_STATE_H
