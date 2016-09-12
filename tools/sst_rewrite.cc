// -------------------------------------------------------------------
//
// sst_rewrite.cc
//
// Copyright (c) 2015-2016 Basho Technologies, Inc. All Rights Reserved.
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

#include <memory>
#include <stdio.h>
#include <stdlib.h>

#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "rewrite_state.h"
#include "ldb_table.h"

#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "leveldb_os/expiry_os.h"

#include "db/dbformat.h"

bool RewriteFile(RewriteState & state);
bool RewriteDatabase(RewriteState & state);

void command_help();


int
main(
    int argc,
    char ** argv)
{
    OptionsIter iter(argv);
    RewriteState state;
    bool keep_going;

    keep_going=true;

    // skip first arg, command name
    ++iter;

    // process next arg
    while(state.ApplyNext(iter) && keep_going)
    {
        switch(state.GetNextAction())
        {
            case RewriteState::eDoNothing:
                break;

            case RewriteState::eRewriteFile:
                keep_going=RewriteFile(state);
                break;

            case RewriteState::eRewriteDir:
                keep_going=RewriteDatabase(state);
                break;

            default:
                state.SetFail("Unknown action after option parsed (%d).",(int)state.GetNextAction());
                break;
        }   // switch
        keep_going=state.IsGood() && (keep_going || state.KeepGoing());

    }   // while

    if (!state.IsGood() && state.IsVerbose())
        command_help();

    return(state.IsGood() ? 0 : 1 );

}   // main


bool
RewriteFile(
    RewriteState & State)
{
    bool good, process_file;

    // Options: needs filter & total_leveldb_mem initialized
    leveldb::Options options;

    // using 16 bit width per key in bloom filter
    options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
    // tell leveldb it can use 512Mbyte of memory
    options.total_leveldb_mem=(512 << 20);

    good=true;
    process_file=false;

    LDbTable in_file(options, State.GetString(0));

    if (in_file.GetStatus().ok())
    {
        process_file=State.IsForce()
            || (State.IsPurgeExpiry() && in_file.HasExpiry())
            || (State.CompressionType() < in_file.CompressionType());

        if (process_file)
            State.Log(" +++ Rewriting file %s", State.GetString(0).c_str());
        else
            State.Log(" --- Skipping file %s", State.GetString(0).c_str());

        State.Log("     HasExpiry %s, CompressionType %d",
                  in_file.HasExpiry() ? "true" : "false",
                  in_file.CompressionType());
    }   // if
    else
    {
        State.Log("%s: Input table open failed (%s)\n",
                  State.GetString(0).c_str(), in_file.GetStatus().ToString().c_str());
        good=false;
    }   // else

    if (process_file && !State.IsTrialRun())
    {
        std::string fname;
        leveldb::WritableFile * outfile;
        leveldb::Status s;
        std::auto_ptr<leveldb::Iterator> it;
        std::auto_ptr<leveldb::TableBuilder> builder;

        it.reset(in_file.NewIterator());

        fname=State.GetString(0);
        fname.append(".new");

        outfile=NULL;
        s = options.env->NewWritableFile(fname, &outfile,
                                         options.env->RecoveryMmapSize(&options));
        if (s.ok())
        {
            leveldb::ParsedInternalKey parsed_key;
            leveldb::InternalKey clean_key;

            // set compression and expiry
            //  (expiry module needed even for cleaning)
            options.expiry_module.reset(new leveldb::ExpiryModuleOS);
            options.compression=State.CompressionType();
            options.block_size=State.GetBlockSize();

            builder.reset(new leveldb::TableBuilder(options, outfile));

            for (it->SeekToFirst();
                 it->Valid() && s.ok() && builder->status().ok() && good;
                 it->Next())
            {
                leveldb::Slice key = it->key();

                // clean expiry from key when required
                if (State.IsPurgeExpiry() && leveldb::IsExpiryKey(key))
                {
                    good=leveldb::ParseInternalKey(key, &parsed_key);
                    if (good)
                    {
                        parsed_key.type=leveldb::kTypeValue;
                        clean_key.SetFrom(parsed_key);
                        key=clean_key.internal_key();
                    }   // if
                    else
                    {
                        State.Log("ParseInternalKey failed.");
                    }   // else
                }   // if

                if (good)
                    builder->Add(key, it->value());
            }   // for

            // hmmm, nothing new setting status right now.
            if (s.ok() && builder->status().ok() && good)
                s = builder->Finish();
            else
                builder->Abandon();
        }   // if
        else
        {
            // Table::Open failed on file "fname"
            State.Log(" *** %s: NewWritableFile failed (%s)\n",
                      fname.c_str(), s.ToString().c_str());
            good=false;
        }   // else

        if (NULL!=outfile)
            outfile->Close();

        delete outfile;
    }   // if
    return(good);

}   // RewriteFile


bool
RewriteDatabase(
    RewriteState & State)
{
    bool good, process_file;

// manifest & current files?
// any .log files
// sst_? directories

// translate all directories, delete old files?
// translate .log files? strip expiry?
// rewrite manifest if file change and/or expiry metadata (compression?)



}   // RewriteDatabase



#if 0
    for (cursor=argv+1;
         NULL!=*cursor && running && !error_seen;
         ++cursor)
    {

            std::string fname;
            fname=*cursor;

            // do a rewrite
            if (!compare_files)
            {
                leveldb::WritableFile * outfile;
                leveldb::Status s;
                std::auto_ptr<leveldb::Iterator> it;
                std::auto_ptr<leveldb::TableBuilder> builder;

                LDbTable in_file(options, fname);

                if (in_file.GetStatus().ok())
                {
                    it.reset(in_file.NewIterator());

                    fname.append(".new");
                    s = options.env->NewWritableFile(fname, &outfile,
                                                     options.env->RecoveryMmapSize(&options));
                    if (s.ok())
                        builder.reset(new leveldb::TableBuilder(options, outfile));
                    else
                    {
                        // Table::Open failed on file "fname"
                        fprintf(stderr, "%s: NewWritableFile failed (%s)\n",
                                fname.c_str(), s.ToString().c_str());
                        error_seen=true;
                    }   // else

                    for (it->SeekToFirst();
                         it->Valid() && s.ok() && builder->status().ok();
                         it->Next())
                    {
                        leveldb::Slice key = it->key();
                        builder->Add(key, it->value());
                    }   // for

                    // hmmm, nothing new setting status right now.
                    if (s.ok() && builder->status().ok()) {
                        s = builder->Finish();
                    } else {
                        builder->Abandon();
                    }

                    if (NULL!=outfile)
                        outfile->Close();
                    delete outfile;
                }   // if
                else
                {
                    fprintf(stderr, "%s: Input table open failed (%s)\n",
                            fname.c_str(), in_file.GetStatus().ToString().c_str());
                    error_seen=true;
                }   // else
            }   // if

            // compare two files
            else
            {
                LDbTable file1(options, fname);

                ++cursor;
                if (NULL!=*cursor)
                {
                    fname=*cursor;
                    LDbTable file2(options, fname);

                    if (file1.GetStatus().ok() && file2.GetStatus().ok())
                    {
                        // quick check: same number of keys and bytes of user data?
                        //     do this before reading entire files
                        if (file1.GetSstCounter(leveldb::eSstCountKeys)==file2.GetSstCounter(leveldb::eSstCountKeys)
                            && file1.GetSstCounter(leveldb::eSstCountKeySize)==file2.GetSstCounter(leveldb::eSstCountKeySize)
                            && file1.GetSstCounter(leveldb::eSstCountValueSize)==file2.GetSstCounter(leveldb::eSstCountValueSize))
                        {
                            leveldb::Iterator * it1, *it2;
                            uint64_t key_count;
                            bool match;

                            it1=file1.NewIterator();
                            it2=file2.NewIterator();
                            match=true;

                            for (it1->SeekToFirst(), it2->SeekToFirst(), key_count=1;
                                 it1->Valid() && it2->Valid() && match;
                                 it1->Next(), it2->Next(), ++key_count)
                            {
                                match=(0==it1->key().compare(it2->key())) && (0==it1->value().compare(it2->value()));

                                if (!match)
                                {
                                    fprintf(stderr, "%s, %s: Content mismatch at key position %d (%d, %d).\n",
                                            file1.GetFileName(), file2.GetFileName(),
                                            (int)key_count,
                                            it1->key().compare(it2->key()), it1->value().compare(it2->value()));
                                    error_seen=true;
                                }   // if

                            }   // for

                            if (it1->Valid() != it2->Valid())
                            {
                                fprintf(stderr, "%s, %s: Walk of keys terminated early (%d, %d).\n",
                                        file1.GetFileName(), file2.GetFileName(),
                                        (int)it1->Valid(), (int)it2->Valid());
                                error_seen=true;
                            }
                        }   // if
                        else
                        {
                            if (file1.GetSstCounter(leveldb::eSstCountKeys)==file2.GetSstCounter(leveldb::eSstCountKeys))
                                fprintf(stderr, "%s, %s: Number of keys different, %" PRIu64 " vs %" PRIu64 ".\n",
                                        file1.GetFileName(), file2.GetFileName(),
                                        file1.GetSstCounter(leveldb::eSstCountKeys),
                                        file2.GetSstCounter(leveldb::eSstCountKeys));

                            if (file1.GetSstCounter(leveldb::eSstCountKeySize)==file2.GetSstCounter(leveldb::eSstCountKeySize))
                                fprintf(stderr, "%s, %s: Byte size of all keys different, %" PRIu64 " vs %" PRIu64 "\n",
                                        file1.GetFileName(), file2.GetFileName(),
                                        file1.GetSstCounter(leveldb::eSstCountKeySize),
                                        file2.GetSstCounter(leveldb::eSstCountKeySize));

                            if (file1.GetSstCounter(leveldb::eSstCountValueSize)==file2.GetSstCounter(leveldb::eSstCountValueSize))
                                fprintf(stderr, "%s, %s: Byte size of all values different, %" PRIu64 " vs %" PRIu64 "\n",
                                        file1.GetFileName(), file2.GetFileName(),
                                        file1.GetSstCounter(leveldb::eSstCountValueSize),
                                        file2.GetSstCounter(leveldb::eSstCountValueSize));
                            error_seen=true;
                        }   // else
                    }   // if
                    else
                    {
                        if (!file1.GetStatus().ok())
                            fprintf(stderr, "%s: Input table open failed (%s)\n",
                                    file1.GetFileName(), file1.GetStatus().ToString().c_str());
                        if (!file2.GetStatus().ok())
                            fprintf(stderr, "%s: Input table open failed (%s)\n",
                                    file2.GetFileName(), file2.GetStatus().ToString().c_str());
                        error_seen=true;
                    }   // else
                }   // if
                else
                {
                    fprintf(stderr, "%s: compare needs two file names, only have one\n",
                            fname.c_str());
                }   // else
            }   // else
        }   // else
    }   // for

    // cleanup
    options.env->Shutdown();
    delete options.filter_policy;

    if (1==argc)
        command_help();

    return( error_seen ? 1 : 0 );
#endif


void
command_help()
{
    fprintf(stderr, "sst_rewrite [option | file]*\n");
    fprintf(stderr, "  options\n");
    fprintf(stderr, "      -b  value  set Options.block_size to value\n");
    fprintf(stderr, "      -n  set Options.compression to No compression\n");
    fprintf(stderr, "      -s  set Options.compression to Snappy compression\n");
    fprintf(stderr, "      -z  set Options.compression to LZ4 compression (default)\n");
    fprintf(stderr, "      -c  compare next two files (inverse of -w)\n");
    fprintf(stderr, "      -w  rewrite next file (default, inverse of -c)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "      -d <path> rewrite entire database (vnode):  files and manifest\n");
    fprintf(stderr, "      -p <path> rewrite entire \"platform data\" (all vnodes), files and manifest\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "      -t <number> <fast_path> <slow_path>\n");
    fprintf(stderr, "         <number> first slow tier level\n");
    fprintf(stderr, "         <fast_path> \"fast path\" of tiered storage\n");
    fprintf(stderr, "         <slow_path> \"slow path\" of tiered storage\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "      -x purge expiry data\n");
    fprintf(stderr, "      -m manifest only\n");
    fprintf(stderr, "      -k keep processing files even when some files fail\n");
    fprintf(stderr, "      -f force rewrite even if compression/expiry ok\n");
    fprintf(stderr, "      -N only list files and expected rewrite, no real conversion\n");
    fprintf(stderr, "      -v verbose reporting (default)\n");
    fprintf(stderr, "      -q quiet mode, no reporting\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "      -g <app.timestamp.config> NOT IMPLEMENTED previously generated config for this node\n");
    fprintf(stderr, "\n");
}   // command_help

namespace leveldb {


}  // namespace leveldb

