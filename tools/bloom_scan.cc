#include <stdlib.h>
#include <libgen.h>

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/iterator.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "table/format.h"
#include "table/block.h"
#include "table/filter_block.h"

//#include "util/logging.h"
//#include "db/log_reader.h"

void command_help();

int
main(
    int argc,
    char ** argv)
{
    bool error_seen, all_keys, csv_header, running, no_csv;
    int counter, error_counter;
    char ** cursor;

    running=true;
    error_seen=false;

    csv_header=false;
    all_keys=false;
    no_csv=false;
    counter=0;
    error_counter=0;


    for (cursor=argv+1; NULL!=*cursor && running; ++cursor)
    {
        // option flag?
        if ('-'==**cursor)
        {
            char flag;

            flag=*((*cursor)+1);
            switch(flag)
            {
                case 'h':  csv_header=true; break;
                case 'k':  all_keys=true; break;
                case 'n':  no_csv=true; break;
                default:
                    fprintf(stderr, " option \'%c\' is not valid\n", flag);
                    command_help();
                    running=false;
                    error_counter=1;
                    error_seen=true;
                    break;
            }   // switch
        }   // if

        // database path
        else
        {
            leveldb::DB * db_ptr;
            leveldb::Options options;
            leveldb::ReadOptions read_options;
            std::string dbname;
            leveldb::Env * env;
            leveldb::Status status;
            size_t get_errors, key_count;
            uint64_t timer;

            get_errors=0;
            key_count=0;
            env=leveldb::Env::Default();

            dbname=*cursor;
            options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
            options.env=env;
            options.max_open_files=250;

            read_options.verify_checksums=true;
            read_options.fill_cache=false;     // force bloom to be used

            db_ptr=NULL;
            timer=env->NowMicros();

            status=leveldb::DB::Open(options, dbname, &db_ptr);

            if (status.ok())
            {
                leveldb::Iterator * it;
                std::string value;

                it=NULL;
                it=db_ptr->NewIterator(read_options);
                for (it->SeekToFirst(); it->status().ok() && it->Valid(); it->Next())
                {
                    ++key_count;

                    if (all_keys)
                    {
                        leveldb::ParsedInternalKey parsed;
                        leveldb::Slice key = it->key();

                        ParseInternalKey(key, &parsed);
                        printf("%s db %s\n", parsed.DebugStringHex().c_str(), dbname.c_str());
                    }   // if

                    status=db_ptr->Get(read_options, it->key(), &value);
                    if (!status.ok())
                        ++get_errors;
                }   // for
                timer=env->NowMicros() - timer;

                if (!no_csv)
                {
                    if (csv_header)
                    {
                        csv_header=false;
                        printf("database, timer micros, key count, GET errors\n");
                    }   // if

                    printf("%s, %llu, %zd, %zd\n",
                           dbname.c_str(), timer, key_count, get_errors);
                }   // if

                delete it;
                delete db_ptr;
            }   // if
            else
            {
                fprintf(stderr, "%s:  leveldb::Open() failed (%s)",
                        dbname.c_str(), status.ToString().c_str());
                error_seen=true;
                error_counter+=1;
            }   // else
        }   // else
    }   // for

    if (1==argc)
        command_help();

    return( error_seen && 0!=error_counter ? 1 : 0 );

}   // main


void
command_help()
{
    fprintf(stderr, "bloom_scan [option | data_base]*\n");
    fprintf(stderr, "  options\n");
    fprintf(stderr, "      -h  print csv formatted header line (once)\n");
    fprintf(stderr, "      -k  print all keys\n");
    fprintf(stderr, "      -n  NO csv data (or header)\n");
}   // command_help

namespace leveldb {


}  // namespace leveldb

