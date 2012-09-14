#include <stdlib.h>
#include <libgen.h>

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/db.h"
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "table/format.h"
#include "table/block.h"
#include "table/filter_block.h"

//#include "util/logging.h"
//#include "db/log_reader.h"

int
main(
    int argc,
    char ** argv)
{
    bool error_seen, index_keys, all_keys, block_info, csv_header, counter_info;
    int counter, error_counter;
    char ** cursor;

    error_seen=false;

    block_info=false;
    counter_info=false;
    index_keys=false;
    csv_header=false;
    all_keys=false;
    counter=0;
    error_counter=0;


    for (cursor=argv+1; NULL!=*cursor; ++cursor)
    {
        // option flag?
        if ('-'==**cursor)
        {
            char flag;

            flag=*((*cursor)+1);
            switch(flag)
            {
                case 'b':  block_info=true; break;
                case 'c':  counter_info=true; break;
                case 'h':  csv_header=true; break;
                case 'i':  index_keys=true; break;
                case 'k':  all_keys=true; break;
                default:
                    printf(" option \'%c\' is not valid\n", flag);
                    break;
            }   // switch
        }   // if

        // sst file
        else
        {
            leveldb::Options options;
            leveldb::ReadOptions read_options;
            std::string table_name, dbname, path_temp;
            leveldb::Env * env;
            leveldb::FileMetaData meta;
            leveldb::TableCache * table_cache;
            env=leveldb::Env::Default();

            // make copy since basename() and dirname() may modify
            path_temp=*cursor;
            dbname=dirname((char *)path_temp.c_str());
            path_temp=*cursor;
            table_name=basename((char *)path_temp.c_str());
            meta.number=strtol(table_name.c_str(), NULL, 10);

            options.filter_policy=leveldb::NewBloomFilterPolicy(10);
            table_cache=new leveldb::TableCache(dbname, &options, 10);
            table_name = leveldb::TableFileName(dbname, meta.number);

            // open table, step 1 get file size
            leveldb::Status status = env->GetFileSize(table_name, &meta.file_size);
            if (!status.ok())
            {
                printf("%s: GetFileSize failed (%s)\n", table_name.c_str(),status.ToString().c_str());
                error_seen=true;
                error_counter=10;
            }   // if

            //open table, step 2 find table (cache or open)
            if (status.ok())
            {
                leveldb::Cache::Handle * handle;

                handle=NULL;
                status=table_cache->TEST_FindTable(meta.number, meta.file_size, &handle);

                // count keys and size keys/filter
                if (status.ok())
                {
                    leveldb::Table* table;
                    leveldb::Iterator *it, *it2;
                    int count, count2, total, block_count;
                    size_t tot_size, smallest_block, tot_compress, tot_uncompress;
                    bool first;
                    leveldb::Status status;
                    leveldb::RandomAccessFile * file;

                    total=0;
                    count=0;
                    count2=0;
                    tot_size=0;

                    table = reinterpret_cast<leveldb::TableAndFile*>(table_cache->TEST_GetInternalCache()->Value(handle))->table;
                    file = reinterpret_cast<leveldb::TableAndFile*>(table_cache->TEST_GetInternalCache()->Value(handle))->file;
                    it = table->TEST_GetIndexBlock()->NewIterator(options.comparator);

//                if (NULL!=table->FilterObject())
//                    table->FilterObject()->Dump();

                    // walk keys in index block
                    if (index_keys)
                    {
                        for (it->SeekToFirst(), count=0; it->Valid(); it->Next())
                        {
                            ++count;
                            if (it->status().ok())
                            {
                                leveldb::ParsedInternalKey parsed;
                                leveldb::Slice key = it->key();
                                leveldb::Slice value = it->value();

                                ParseInternalKey(key, &parsed);
                                printf("key %zd, value %zd: %s\n", key.size(), value.size(), parsed.DebugStringHex().c_str());
                            }   // if
                            else
                            {
                                printf("%s: index iterator failed (%s)\n", table_name.c_str(),it->status().ToString().c_str());
                            }   // else
                        }   // for
                    }   // if

                    // Walk all blocks (but nothing within block)
                    smallest_block=0;
                    first=true;
                    block_count=0;
                    tot_compress=0;
                    tot_uncompress=0;

                    for (it->SeekToFirst(), count=0; it->Valid(); it->Next())
                    {
                        leveldb::BlockContents contents;
                        leveldb::BlockHandle handle;
                        leveldb::Slice slice;

                        ++block_count;
                        slice=it->value();
                        handle.DecodeFrom(&slice);

                        if (block_info)
                        {
                            printf("block %d, offset %zd, size %zd, next %zd\n",
                                   block_count, handle.offset(), handle.size(), handle.offset()+handle.size());
                        }   // if

                        tot_compress+=handle.size();
                        status=leveldb::ReadBlock(file, read_options, handle, &contents);
                        if (status.ok())
                        {
                            if (first)
                            {
                                first=false;
                                smallest_block=contents.data.size();
                            }   // if
                            else if (contents.data.size()<smallest_block)
                            {
                                smallest_block=contents.data.size();
                            }   // else if
                            tot_uncompress+=contents.data.size();
                        }   // if
                        else
                        {
                            printf("ReadBlock failed on block %d\n", block_count);
                        }   // else
                    }   // for

                    // Walk all keys in each block.
                    for (it->SeekToFirst(), count=0; it->Valid(); it->Next())
                    {
                        ++count;
                        it2=leveldb::Table::TEST_BlockReader(table, read_options, it->value());
                        for (it2->SeekToFirst(), count2=0; it2->Valid(); it2->Next())
                        {
                            ++count2;
                            ++total;
                            if (it2->status().ok())
                            {
                                tot_size+=it2->value().size();

                                if (all_keys)
                                {
                                    leveldb::ParsedInternalKey parsed;
                                    leveldb::Slice key = it2->key();
                                    leveldb::Slice value = it2->value();

                                    ParseInternalKey(key, &parsed);
                                    printf("%s block_key %s\n", parsed.DebugStringHex().c_str(), table_name.c_str());
                                }   // if
                            }   // if
                            else
                            {
                                printf("%s: value iterator failed, location [%d, %d] (%s)\n",
                                       table_name.c_str(),count, count2,it2->status().ToString().c_str());
                            }   // else
                        }   // for
                    }   // for

                    if (csv_header)
                    {
                        csv_header=false;
                        printf("Table File, File size, Index size, Index key count, ");
                        printf("total key count, total value size, average value size, smallest block, ratio*100, ");
                        printf("table object size, filter size");

                        if (counter_info)
                        {
                            unsigned loop;
                            leveldb::SstCounters counters;

                            counters=table->GetSstCounters();

                            for (loop=0; loop<counters.Size(); ++loop)
                                printf(", Counter %u", loop);
                        }   // if

                        printf("\n");
                    }   // if

                    printf("%s, %zd, %zd, %d,",
                           table_name.c_str(), meta.file_size, table->TEST_GetIndexBlock()->size(), count);

                    printf(" %d, %zd, %zd, %zd, %zd,",
                           total, tot_size, (0!=count2) ? tot_size/total : 0, smallest_block,
                           (tot_uncompress*100)/tot_compress);

                    printf(" %zd, %zd",
                           table->TEST_TableObjectSize(), table->TEST_FilterDataSize());

                    if (counter_info)
                    {
                        unsigned loop;
                        leveldb::SstCounters counters;

                        counters=table->GetSstCounters();

                        for (loop=0; loop<counters.Size(); ++loop)
                            printf(", %zu", counters.Value(loop));
                    }   // if

                    printf("\n");

                }   // if
                else
                {
                    printf("%s: FindTable failed (%s)\n", table_name.c_str(),status.ToString().c_str());
                    error_seen=true;
                    error_counter=10;
                }   // else
            }   // if

#if 0
            if (status.ok())
            {
                leveldb::Iterator* iter = table_cache->NewIterator(
                    read_options, meta.number, meta.file_size);

                for (iter->SeekToFirst(); iter->Valid(); iter->Next())
                {
#if 0
                    leveldb::ParsedInternalKey parsed;
                    leveldb::Slice key = iter->key();
                    leveldb::Slice value = iter->value();

                    printf("key %zd, value %zd\n", key.size(), value.size());
#endif
#if 0
                    if (!ParseInternalKey(key, &parsed)) {
                        Log(options_.info_log, "Table #%llu: unparsable key %s",
                            (unsigned long long) meta.number,
                            EscapeString(key).c_str());
                        continue;
                    }
#endif

                    if (!iter->status().ok() && !error_seen)
                    {
                        error_counter=counter;
                        printf("%s: error at counter %d of ", table_name.c_str(), counter);
                        error_seen=true;
                    }   // if

                    counter++;

                }   //for

                if (error_seen)
                {
                    printf("%d\n", counter);
                }   // if
                else if (!iter->Valid() && 0==counter)
                {
                    printf("%s: not valid iterator\n", table_name.c_str());
                    error_seen=true;
                    error_counter=10;
                }   // else

                delete iter;
            }   // if
#endif
        }   // else
    }   // for
#if 0
    else
    {
        printf("command:  sst_scan.e path_name_of_sst\n");
        error_seen=true;
        error_counter=10;
    }
#endif

    return( error_seen && 0!=error_counter ? 1 : 0 );

}   // main


namespace leveldb {


}  // namespace leveldb

