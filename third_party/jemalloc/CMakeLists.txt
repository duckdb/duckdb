cmake_minimum_required(VERSION 2.8.12)

if (POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif ()

include_directories(include)

add_library(
    duckdb_jemalloc
    STATIC
    src/arena.c
    src/background_thread.c
    src/base.c
    src/bin.c
    src/bin_info.c
    src/bitmap.c
    src/buf_writer.c
    src/cache_bin.c
    src/ckh.c
    src/counter.c
    src/ctl.c
    src/decay.c
    src/div.c
    src/ecache.c
    src/edata.c
    src/edata_cache.c
    src/ehooks.c
    src/emap.c
    src/eset.c
    src/exp_grow.c
    src/extent.c
    src/extent_dss.c
    src/extent_mmap.c
    src/fxp.c
    src/hook.c
    src/hpa.c
    src/hpa_hooks.c
    src/hpdata.c
    src/inspect.c
    src/jemalloc.c
    src/jemalloc_cpp.cpp
    src/large.c
    src/log.c
    src/malloc_io.c
    src/mutex.c
    src/nstime.c
    src/pa.c
    src/pa_extra.c
    src/pac.c
    src/pages.c
    src/pai.c
    src/peak_event.c
    src/prof.c
    src/prof_data.c
    src/prof_log.c
    src/prof_recent.c
    src/prof_stats.c
    src/prof_sys.c
    src/psset.c
    src/rtree.c
    src/safety_check.c
    src/san.c
    src/san_bump.c
    src/sc.c
    src/sec.c
    src/stats.c
    src/sz.c
    src/tcache.c
    src/test_hooks.c
    src/thread_event.c
    src/ticker.c
    src/tsd.c
    src/witness.c)

target_compile_definitions(duckdb_jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

# Need to include this so that RTLD_NEXT is defined
target_compile_options(duckdb_jemalloc PRIVATE -D_GNU_SOURCE)

target_include_directories(duckdb_jemalloc PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(duckdb_jemalloc PROPERTIES EXPORT_NAME duckdb_jemalloc)

install(TARGETS duckdb_jemalloc
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_jemalloc)
