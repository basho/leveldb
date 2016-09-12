// -------------------------------------------------------------------
//
// rewrite_state.cc
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

#include <stdio.h>
#include <stdlib.h>

#include "rewrite_state.h"
#include "db/dbformat.h"

RewriteState::RewriteState()
    : m_Good(true), m_NextAction(eDoNothing),
      m_BlockSize(4096), m_FirstSlowTier(0),
      m_Compression(leveldb::kLZ4Compression), m_IsRewrite(true), m_PurgeExpiry(false),
      m_ManifestOnly(false), m_Verbose(true), m_TrialRun(false),
      m_ForceRewrite(false), m_KeepGoing(false)
{};


bool
RewriteState::ApplyNext(
    OptionsIter &Option)
{
    bool ret_flag;

    // ret_flag indicates whether to keep going or not
    if (eEnd!=m_NextAction)
        ret_flag=m_Good;
    else
        ret_flag=false;

    // only do work if still in valid state
    if (ret_flag)
    {
        m_NextAction=eDoNothing;

        if (NULL!=*Option)
        {
            int strings_needed, loop;
            const char * ptr(*Option);

            strings_needed=0;

            // is this option a flag
            if ('-' == *ptr)
            {
                bool more;

                ++ptr;
                more=true;

                while('\0'!=*ptr && more && m_Good)
                {
                    switch(*ptr)
                    {
                        case 'b':
                            ++Option;
                            more=false;
                            if (NULL!=*Option)
                                m_BlockSize=strtol(*Option, NULL, 10);
                            else
                                SetFail("Missing block size.");

                            if (m_BlockSize<128 || m_BlockSize>(4<<10))
                                SetFail("Block size out of range.");
                            break;

                        case 'n': m_Compression=leveldb::kNoCompression; break;
                        case 's': m_Compression=leveldb::kSnappyCompression; break;
                        case 'z': m_Compression=leveldb::kLZ4Compression; break;

                        case 'w': m_IsRewrite=true; break;
                        case 'c': m_IsRewrite=false; break;

                        case 'x': m_PurgeExpiry=true; break;
                        case 'm': m_ManifestOnly=true; break;
                        case 'N': m_TrialRun=true; break;
                        case 'f': m_ForceRewrite=true; break;
                        case 'k': m_KeepGoing=true; break;

                        case 'd': m_NextAction=eRewriteDir; strings_needed=1; more=false; break;
                        case 'p': m_NextAction=eRewritePlatform; strings_needed=1; more=false; break;

                        case 't':
                            ++Option;
                            more=false;
                            m_NextAction=eRewriteTiered;
                            if (NULL!=*Option)
                                m_FirstSlowTier=strtol(*Option, NULL, 10);
                            else
                                SetFail("Missing tiered storage options.");

                            if (m_FirstSlowTier<1 || m_FirstSlowTier>(leveldb::config::kNumLevels-1))
                                SetFail("First slow tier value out of range.");
                            strings_needed=2;
                            break;

                        case 'v': m_Verbose=true; break;
                        case 'q': m_Verbose=false; break;

                        default:
                            more=false;
                            SetFail("Unknown option.");
                            break;
                    }   // switch
                    ++ptr;
                }   // while

                if ('\0'!=*ptr && !more)
                    SetFail("Extra options after path option.");

            }   // if

            // assume this is an individual file
            else
            {
                if (m_IsRewrite)
                {
                    strings_needed=1;
                    m_NextAction=eRewriteFile;
                }   // if
                else
                {
                    strings_needed=2;
                    m_NextAction=eCompareFiles;
                }   // else
            }   // else

            loop=0;
            while(0!=strings_needed && m_Good)
            {
                ++Option;
                if (NULL!=*Option)
                    m_String[loop]=*Option;
                else
                    SetFail("Missing path parameter.");

                --strings_needed;
                ++loop;
            }   // while
        }   // if

        // end of options array
        else
        {
            m_NextAction=eEnd;
        }   // else

        ret_flag=m_Good;
    }   // if

    if (ret_flag)
        ++Option;

    return(ret_flag);

}   // RewriteState::ApplyNext


void
RewriteState::SetFail(
    const char * Format,
    ...)
{
    va_list ap;

    m_Good=false;

    va_start(ap, Format);
    vfprintf(stderr, Format, ap);
    fprintf(stderr, "\n");
    va_end(ap);

    return;

}   // RewriteState::SetFail


void
RewriteState::Log(
    const char * Format,
    ...)
{
    va_list ap;

    if (m_Verbose)
    {
        va_start(ap, Format);
        vfprintf(stdout, Format, ap);
        fprintf(stdout, "\n");
        va_end(ap);
    }   // if

    return;

}   // RewriteState::Log
