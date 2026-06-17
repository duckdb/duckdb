include_directories(include)

set(JEMALLOC_C_FILES
  src/jemalloc.c
  src/arena.c
  src/background_thread.c
  src/base.c
  src/batcher.c
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
  src/san.c
  src/san_bump.c
  src/hook.c
  src/hpa.c
  src/hpa_hooks.c
  src/hpdata.c
  src/inspect.c
  src/large.c
  src/log.c
  src/malloc_io.c
  src/mutex.c
  src/nstime.c
  src/pa.c
  src/pa_extra.c
  src/pai.c
  src/pac.c
  src/pages.c
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
  src/sc.c
  src/sec.c
  src/stats.c
  src/sz.c
  src/tcache.c
  src/test_hooks.c
  src/thread_event.c
  src/ticker.c
  src/tsd.c
  src/util.c
  src/witness.c
  src/zone.c)

# if we add this jemalloc overrides new/delete
# would be great but causes trouble in our Python client
# so we only add it as an optional build parameter
if(OVERRIDE_NEW_DELETE)
    set(JEMALLOC_C_FILES ${JEMALLOC_C_FILES} src/jemalloc_cpp.cpp)
endif()

add_library(duckdb_jemalloc STATIC ${JEMALLOC_C_FILES})

set_property(TARGET duckdb_jemalloc PROPERTY C_STANDARD 99)

target_include_directories(duckdb_jemalloc PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(duckdb_jemalloc PROPERTIES EXPORT_NAME duckdb_jemalloc)

install(TARGETS duckdb_jemalloc
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_jemalloc)
