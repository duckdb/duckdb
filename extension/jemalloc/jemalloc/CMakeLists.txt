include_directories(include)

add_library(
  jemalloc OBJECT
  src/arena.cpp
  src/background_thread.cpp
  src/base.cpp
  src/bin.cpp
  src/bin_info.cpp
  src/bitmap.cpp
  src/buf_writer.cpp
  src/cache_bin.cpp
  src/ckh.cpp
  src/counter.cpp
  src/ctl.cpp
  src/decay.cpp
  src/div.cpp
  src/ecache.cpp
  src/edata.cpp
  src/edata_cache.cpp
  src/ehooks.cpp
  src/emap.cpp
  src/eset.cpp
  src/exp_grow.cpp
  src/extent.cpp
  src/extent_dss.cpp
  src/extent_mmap.cpp
  src/fxp.cpp
  src/hook.cpp
  src/hpa.cpp
  src/hpa_hooks.cpp
  src/hpdata.cpp
  src/inspect.cpp
  src/jemalloc.cpp
  src/large.cpp
  src/log.cpp
  src/malloc_io.cpp
  src/mutex.cpp
  src/nstime.cpp
  src/pa.cpp
  src/pa_extra.cpp
  src/pac.cpp
  src/pages.cpp
  src/pai.cpp
  src/peak_event.cpp
  src/prof.cpp
  src/prof_data.cpp
  src/prof_log.cpp
  src/prof_recent.cpp
  src/prof_stats.cpp
  src/prof_sys.cpp
  src/psset.cpp
  src/rtree.cpp
  src/safety_check.cpp
  src/san.cpp
  src/san_bump.cpp
  src/sc.cpp
  src/sec.cpp
  src/stats.cpp
  src/sz.cpp
  src/tcache.cpp
  src/test_hooks.cpp
  src/thread_event.cpp
  src/ticker.cpp
  src/tsd.cpp
  src/witness.cpp)

# we don't want this
target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)
target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_RENAME)

# some extra debug defs
if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG)
  target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_LOG)
endif()

set(JEMALLOC_OBJECT_FILES
    ${JEMALLOC_OBJECT_FILES} $<TARGET_OBJECTS:jemalloc>
    PARENT_SCOPE)

disable_target_warnings(jemalloc)
