project(leveldb)

# OS detection
add_definitions(-DLEVELDB_PLATFORM_POSIX -DSNAPPY -mmacosx-version-min=10.8)
if(APPLE)
    add_definitions(-DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    add_definitions(-DOS_FREEBSD)
endif()

# leveldb source files
#
# NOTE: selected header files are included to make CLion happy
include_directories(${CMAKE_CURRENT_SOURCE_DIR} include)
set(LEVELDB_SRCS
    db/builder.cc
    db/db_impl.cc
    db/db_iter.cc
    db/c.cc
    db/filename.cc
    db/dbformat.cc
    db/log_format.h
    db/log_reader.cc
    db/log_writer.cc
    db/memtable.cc
    db/repair.cc
    db/skiplist.h
    db/snapshot.h
    db/table_cache.cc
    db/version_edit.cc
    db/version_set.cc
    db/write_batch.cc
    db/write_batch_internal.h
    include/leveldb/atomics.h
    include/leveldb/c.h
    include/leveldb/cache.h
    include/leveldb/comparator.h
    include/leveldb/db.h
    include/leveldb/env.h
    include/leveldb/filter_policy.h
    include/leveldb/iterator.h
    include/leveldb/options.h
    include/leveldb/perf_count.h
    include/leveldb/slice.h
    include/leveldb/status.h
    include/leveldb/table.h
    include/leveldb/table_builder.h
    include/leveldb/write_batch.h
    port/atomic_pointer.h
    port/port.h
    port/port_posix.cc
    table/block.cc
    table/block_builder.cc
    table/filter_block.cc
    table/format.cc
    table/iterator.cc
    table/iterator_wrapper.h
    table/merger.cc
    table/table.cc
    table/table_builder.cc
    table/two_level_iterator.cc
    util/arena.cc
    util/bloom.cc
    util/bloom2.cc
    util/cache.cc
    util/cache2.cc
    util/coding.cc
    util/comparator.cc
    util/crc32c.cc
    util/db_list.cc
    util/env.cc
    util/env_posix.cc
    util/filter_policy.cc
    util/flexcache.cc
    util/hash.cc
    util/histogram.cc
    util/hot_threads.cc
    util/logging.cc
    util/murmurhash.cc
    util/mutexlock.h
    util/options.cc
    util/perf_count.cc
    util/posix_logger.h
    util/random.h
    util/status.cc
    util/throttle.cc
    )

add_library(leveldb ${LEVELDB_SRCS})

# TESTS

set(TESTHARNESS_SRCS
    util/testutil.cc
    util/testharness.cc)

add_library(leveldb-testharness STATIC ${TESTHARNESS_SRCS})

macro(leveldb_tests)
    foreach(t ${ARGN})
        get_filename_component(_tname ${t} NAME_WE)
        add_executable(${_tname} ${t})
        target_link_libraries(${_tname}
                leveldb-testharness
                leveldb
                )
    endforeach()
endmacro()

leveldb_tests(
    db/c_test.c
    db/corruption_test.cc
    db/db_bench.cc
    db/db_test.cc
    db/dbformat_test.cc
    db/filename_test.cc
    db/log_test.cc
    db/skiplist_test.cc
    db/version_edit_test.cc
    db/version_set_test.cc
    db/write_batch_test.cc
    table/table_test.cc
    table/filter_block_test.cc
    util/arena_test.cc
    util/bloom_test.cc
    util/cache_test.cc
    util/cache2_test.cc
    util/coding_test.cc
    util/crc32c_test.cc
    util/env_test.cc
    util/flexcache_test.cc
    util/perf_count_test.cc
    )
