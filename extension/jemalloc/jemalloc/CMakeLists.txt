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
# should be resolvable, with some effort
# src/jemalloc_cpp.cpp

add_library(jemalloc OBJECT ${JEMALLOC_C_FILES})

set_property(TARGET jemalloc PROPERTY C_STANDARD 99)

set(JEMALLOC_OBJECT_FILES
    ${JEMALLOC_OBJECT_FILES} $<TARGET_OBJECTS:jemalloc>
    PARENT_SCOPE)

disable_target_warnings(jemalloc)
