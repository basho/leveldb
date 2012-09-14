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
    bool error_seen;
    int counter, error_counter;

    error_seen=false;
    counter=0;
    error_counter=0;

    if (2==argc)
    {
        leveldb::Options options;
        leveldb::ReadOptions read_options;
        std::string table_name, dbname, path_temp;
        leveldb::Env * env;
        leveldb::FileMetaData meta;
        leveldb::TableCache * table_cache;
        env=leveldb::Env::Default();

        //read_options.absorb_decompress_errors=false;

        // command line params
        // dbname="/home/mmaszewski/basho/zd_logs/2163";
        // dbname=argv[1];
        // meta.number=strtol(argv[2], NULL, 10);

        // make copy since basename() and dirname() may modify
        path_temp=argv[1];
        dbname=dirname((char *)path_temp.c_str());
        path_temp=argv[1];
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

                printf("Table object size: %zd, filter data size: %zd\n",
                       table->TEST_TableObjectSize(), table->TEST_FilterDataSize());
//                if (NULL!=table->FilterObject())
//                    table->FilterObject()->Dump();

                // walk keys in index block
                for (it->SeekToFirst(), count=0; it->Valid(); it->Next())
                {
                    ++count;
                    if (it->status().ok())
                    {
                        leveldb::ParsedInternalKey parsed;
                        leveldb::Slice key = it->key();
                        leveldb::Slice value = it->value();

                        ParseInternalKey(key, &parsed);
                        printf("key %zd, value %zd: %s\n", key.size(), value.size(), parsed.DebugString().c_str());
                    }   // if
                    else
                    {
                        printf("%s: index iterator failed (%s)\n", table_name.c_str(),it->status().ToString().c_str());
                    }   // else
                }   // for

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
#if 0
                    printf("block %d, offset %zd, size %zd, next %zd\n",
                           block_count, handle.offset(), handle.size(), handle.offset()+handle.size());
#endif
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

                printf("\nAll:\n");
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

                            leveldb::ParsedInternalKey parsed;
                            leveldb::Slice key = it2->key();
                            leveldb::Slice value = it2->value();

                            ParseInternalKey(key, &parsed);
                            printf("%s xxblock_keyxx %s\n", parsed.DebugString().c_str(), table_name.c_str());

                        }   // if
                        else
                        {
                            printf("%s: value iterator failed, location [%d, %d] (%s)\n",
                                   table_name.c_str(),count, count2,it2->status().ToString().c_str());
                        }   // else
                    }   // for
                }   // for

#if 0
                printf("%s: File size %zd, Index size %zd, Index key count %d,",
                       table_name.c_str(), meta.file_size, table->TEST_GetIndexBlock()->size(), count);

                printf("total key count %d, total value size %zd, average value size %zd, smallest block %zd, ratio*100 %zd\n",
                       total, tot_size, (0!=count2) ? tot_size/total : 0, smallest_block, (tot_uncompress*100)/tot_compress);
#else
                printf("%s, %zd, %zd, %d,",
                       table_name.c_str(), meta.file_size, table->TEST_GetIndexBlock()->size(), count);

                printf(" %d, %zd, %zd, %zd, %zd\n",
                       total, tot_size, (0!=count2) ? tot_size/total : 0, smallest_block, (tot_uncompress*100)/tot_compress);

#endif


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
    }   // if
    else
    {
        printf("command:  sst_scan.e path_name_of_sst\n");
        error_seen=true;
        error_counter=10;
    }

    return( error_seen && 0!=error_counter ? 1 : 0 );

}   // main


namespace leveldb {


}  // namespace leveldb

