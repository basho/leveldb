// -------------------------------------------------------------------
//
// sst_rewrite.cc
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

#include <stdio.h>
#include <stdlib.h>
//#include <libgen.h>

#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

void command_help();

int
main(
    int argc,
    char ** argv)
{
    bool error_seen, running;
    int error_counter;
    char ** cursor;

    running=true;
    error_seen=false;
    error_counter=0;

    // Options: needs filter & total_leveldb_mem initialized
    leveldb::Options options;
    options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
    options.total_leveldb_mem=(512 << 20);

    // testing
    options.block_size=16 << 10;

    for (cursor=argv+1; NULL!=*cursor && running; ++cursor)
    {
        // option flag?
        if ('-'==**cursor)
        {
            char flag;

            flag=*((*cursor)+1);
            switch(flag)
            {
                case 'b':  options.block_size=4096; break;
                default:
                    fprintf(stderr, " option \'%c\' is not valid\n", flag);
                    command_help();
                    running=false;
                    error_counter=1;
                    error_seen=true;
                    break;
            }   // switch
        }   // if

        // sst file
        else
        {
            std::string fname;
            leveldb::RandomAccessFile* infile = NULL;
            leveldb::WritableFile *outfile = NULL;
            leveldb::Table* table = NULL;
            uint64_t file_size;
            leveldb::Status s;
            leveldb::Iterator *it;
            leveldb::ReadOptions read_options;
            leveldb::TableBuilder * builder = NULL;

            fname=*cursor;

            if (options.env->GetFileSize(fname, &file_size).ok()
                && options.env->NewRandomAccessFile(fname, &infile).ok())
            {
                s = leveldb::Table::Open(options, infile, file_size, &table);
                if (s.ok())
                {
                    // use fadvise to start file pre-read
                    infile->SetForCompaction(file_size);

                    read_options.fill_cache=false;
                    it=table->NewIterator(read_options);

                    fname.append(".new");
                    s = options.env->NewWritableFile(fname, &outfile,
                                                     options.env->RecoveryMmapSize(&options));
                    if (s.ok())
                        builder = new leveldb::TableBuilder(options, outfile);
                    else
                    {
                        // Table::Open failed on file "fname"
                        fprintf(stderr, "%s: NewWritableFile failed (%s)\n",
                                fname.c_str(), s.ToString().c_str());
                        error_seen=true;
                        error_counter=1;
                    }   // else

                    for (it->SeekToFirst(); it->Valid() && s.ok(); it->Next())
                    {
                        leveldb::Slice key = it->key();
                        builder->Add(key, it->value());
                    }   // for

                    // hmmm, nothing new setting status right now.
                    if (s.ok()) {
                        s = builder->Finish();
                    } else {
                        builder->Abandon();
                    }
                    delete builder;

                    delete it;
                    if (NULL!=outfile)
                        outfile->Close();
                }   // if
                else
                {
                    // Table::Open failed on file "fname"
                    fprintf(stderr, "%s: Table::Open failed (%s)\n",
                            fname.c_str(), s.ToString().c_str());
                    error_seen=true;
                    error_counter=1;
                }   // else

                delete table;
                delete infile;
                delete outfile;
            }
            else
            {
                fprintf(stderr, "%s: GetFileSize or NewRandomAccessFile failed\n",
                        fname.c_str());
                error_seen=true;
                error_counter=10;
                // unable to open input file "fname"
            }   // else
        }   // else
    }   // for

    // cleanup
    options.env->Shutdown();
    delete options.filter_policy;

    if (1==argc)
        command_help();

    return( error_seen && 0!=error_counter ? 1 : 0 );

}   // main


void
command_help()
{
    fprintf(stderr, "sst_rewrite [option | file]*\n");
    fprintf(stderr, "  options\n");
    fprintf(stderr, "      -b  print details about block\n");
}   // command_help

namespace leveldb {


}  // namespace leveldb

