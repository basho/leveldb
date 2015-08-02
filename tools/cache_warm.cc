// -------------------------------------------------------------------
//
// cache_warm.cc
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

#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <memory>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "include/leveldb/env.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "util/coding.h"

void command_help();

// wrapper class for opening and writing a new Cache Object Warming file
class CowFile
{
public:
    CowFile(std::string & DatabasePath, std::string & Record);
    virtual ~CowFile() {};

    const leveldb::Status & GetStatus() const {return(m_LastStatus);};
    const char * GetFileName() const {return(m_FileName.c_str());};

protected:
    std::string m_FileName;
    leveldb::Status m_LastStatus;

private:
    // disable these
    CowFile();
    CowFile(const CowFile &);
    const CowFile operator=(const CowFile&);
};  // class CowFile


CowFile::CowFile(
    std::string & DatabasePath,
    std::string & Record)
{
    leveldb::WritableFile * cow_file;
    leveldb::log::Writer * cow_log;

    m_FileName=leveldb::CowFileName(DatabasePath);
    m_LastStatus = leveldb::Env::Default()->NewWritableFile(m_FileName, &cow_file, 4*1024L);

    if (m_LastStatus.ok())
    {
        cow_log=new leveldb::log::Writer(cow_file);

        m_LastStatus = cow_log->AddRecord(Record);
        delete cow_log;
        delete cow_file;
    }   // if

    return;

}   // CowFile::CowFile


int
main(
    int argc,
    char ** argv)
{
    bool error_seen, running;
    char ** cursor;

    error_seen=false;
    running=true;


    for (cursor=argv+1;
         NULL!=*cursor && running && !error_seen;
         ++cursor)
    {
        // option flag?
        if ('-'==**cursor)
        {
            char flag;

            flag=*((*cursor)+1);
            switch(flag)
            {
                default:
                    fprintf(stderr, " option \'%c\' is not valid\n", flag);
                    command_help();
                    running=false;
                    error_seen=true;
                    break;
            }   // switch
        }   // if

        // file of .sst pathnames
        else
        {
            FILE * list;
            bool ignore_file;
            std::string cur_path, record;
            size_t line_count;
            char buffer[PATH_MAX], vnode[PATH_MAX], file[NAME_MAX], * chr_ptr;
            int level, ret_val;
            uint64_t file_no, file_size;
            struct stat file_stat;

            list=fopen(*cursor, "r");
            line_count=0;

            if (NULL!=list)
            {
                // loop reading lines
                while(NULL!=fgets(buffer, sizeof(buffer), list) && !error_seen)
                {
                    ++line_count;

                    // parse line
                    chr_ptr=(char*)memchr(buffer, '\n', sizeof(buffer));
                    if (NULL!=chr_ptr)
                        *chr_ptr='\0';

                    // get file size, ignore file if missing or otherwise cranky
                    ignore_file=false;
                    ret_val=stat(buffer, &file_stat);
                    if (0==ret_val)
                        file_size=file_stat.st_size;
                    else
                        ignore_file=true;

                    // get file name
                    chr_ptr=basename(buffer);
                    if (NULL!=chr_ptr)
                        strcpy(file, chr_ptr);
                    else
                        error_seen=true;

                    // get sst_X directory name
                    //  this code assumes c library is inserting
                    //  '\0' into the buffer between directory and base
                    chr_ptr=dirname(buffer);
                    if (NULL!=chr_ptr)
                    {
                        // get level number
                        chr_ptr=basename(buffer);
                        if (NULL!=chr_ptr && 0==strncmp(chr_ptr, "sst_", 4))
                        {
                            level=*(chr_ptr+4)-'0';
                            if (level<0 || leveldb::config::kNumLevels<=level)
                                error_seen=true;
                        }   // if

                        // get base directory of database (vnode)
                        //  (likely the copy to vnode is not needed, but feels better
                        chr_ptr=dirname(buffer);
                        if (NULL!=chr_ptr)
                            strcpy(vnode, chr_ptr);
                        else
                            error_seen=true;
                    }   // if

                    // good line
                    if (!error_seen && !ignore_file)
                    {
                        // did vnode change
                        if (0!=cur_path.compare(vnode))
                        {
                            // write previous
                            if (!cur_path.empty())
                            {
                                CowFile cow(cur_path, record);

                                if (!cow.GetStatus().ok())
                                {
                                    fprintf(stderr, "CowFile failed.\n");
                                    error_seen=true;
                                }   // if
                            }   // if

                            record.clear();
                            cur_path=vnode;
                        }   // if

                        // add file info to record
                        if (!error_seen)
                        {
                            file_no=atol(file);
                            leveldb::PutVarint32(&record, leveldb::VersionEdit::kFileCacheObject);
                            leveldb::PutVarint32(&record, level);
                            leveldb::PutVarint64(&record, file_no);
                            leveldb::PutVarint64(&record, file_size);
                        }   // if
                    }   // if
                    else if (!ignore_file)
                    {
                        fprintf(stderr, "Unable to parse line in %s at line number %zd\n",
                                *cursor, line_count);
                    }   // else
                }   // while

                // write last
                if (!error_seen && !cur_path.empty())
                {
                    CowFile cow(cur_path, record);

                    if (!cow.GetStatus().ok())
                    {
                        fprintf(stderr, "CowFile failed.\n");
                        error_seen=true;
                    }   // if
                }   // if

                fclose(list);
                list=NULL;
            }   // if
            else
            {
                error_seen=true;
                fprintf(stderr, "Unable to open file %s (error %d)",
                        *cursor, errno);
            }   // else
        }   // else
    }   // for

    // cleanup
    leveldb::Env::Default()->Shutdown();
//    delete options.filter_policy;

    if (1==argc)
        command_help();

    return( error_seen ? 1 : 0 );

}   // main


void
command_help()
{
    fprintf(stderr, "sst_rewrite [option | file]*\n");
    fprintf(stderr, "  options\n");
    fprintf(stderr, "      -b  value  set Options.block_size to value\n");
    fprintf(stderr, "      -n  set Options.compression to No compression\n");
    fprintf(stderr, "      -s  set Options.compression to Snappy compression\n");
    fprintf(stderr, "      -z  set Options.compression to LZ4 compression\n");
    fprintf(stderr, "      -c  compare next two files (inverse of -w)\n");
    fprintf(stderr, "      -w  rewrite next file (default, inverse of -c)\n");
}   // command_help

namespace leveldb {


}  // namespace leveldb

