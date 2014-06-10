path="/var/db2/riak/db_test"

#loc_path="/panfs/pas12a/vol1/dhd/bashodb" if using tiering use panasas at level 3

# Basho suggestions
#   options.filter_policy=leveldb::NewBloomFilterPolicy2(16);
#             Basho setting Bloom Bits, modified constructor for benchmark using
#             basho NewBloomFilterPolicy2(FLAGS_bloom_bits)
bb=16
#   options.block_cache=leveldb::NewLRUCache(536870912);
cs=536870912
#   options.max_open_files=500;
of=500
#   options.write_buffer_size=62914560;
wbs=62914560
#   options.env=leveldb::Env::Default();

# Threads
t=1

r=100000000
vs=1024

time ./db_bench \
                                                                --db=$path \
                                                                --benchmarks=fillseq,stats \
                                                                --bloom_bits=$bb \
                                                                --compression_ratio=.5 \
                                                                --use_existing_db=0 \
                                                                --num=$r \
                                                                --threads=$t \
                                                                --value_size=$vs \
                                                                --cache_size=$cs \
                                                                --open_files=$of \
                                                                --write_buffer_size=$wbs
time ./db_bench \
                                                                --db=$path \
                                                                --benchmarks=readseq,stats \
                                                                --bloom_bits=$bb \
                                                                --compression_ratio=.5 \
                                                                --use_existing_db=1 \
                                                                --num=$r \
                                                                --threads=$t \
                                                                --value_size=$vs \
                                                                --cache_size=$cs \
                                                                --open_files=$of \
                                                                --write_buffer_size=$wbs
