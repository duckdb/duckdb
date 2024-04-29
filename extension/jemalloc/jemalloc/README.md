# Updating jemalloc

Clone [this](https://github.com/jemalloc/jemalloc), and check out the branch you need.

For convenience:
```sh
export DUCKDB_DIR=<duckdb_dir>
```

Copy jemalloc source files:
```sh
cd <jemalloc_dir>
./configure --with-jemalloc-prefix="duckdb_je_" --with-private-namespace="duckdb_" --without-export
cp -r src/* $DUCKDB_DIR/extension/jemalloc/jemalloc/src/
cp -r include/* $DUCKDB_DIR/extension/jemalloc/jemalloc/include/
cp COPYING $DUCKDB_DIR/extension/jemalloc/jemalloc/LICENSE
```

Remove junk:
```sh
cd $DUCKDB_DIR/extension/jemalloc/jemalloc
find . -name "*.in" -type f -delete
find . -name "*.sh" -type f -delete
find . -name "*.awk" -type f -delete
find . -name "*.txt" -type f -delete
find . -name "*.py" -type f -delete
```

Restore these files:
```sh
git checkout -- \
  include/jemalloc/internal/jemalloc_internal_defs.h \
  include/jemalloc/jemalloc.h \
  CMakeLists.txt
```

The logarithm of the size of a pointer is defined in `jemalloc.h` and `jemalloc_defs.h`, around line 50.
This is not portable, but we can make it portable if we replace all of it with this:
```c++
#ifdef _MSC_VER
#  ifdef _WIN64
#    define LG_SIZEOF_PTR_WIN 3
#  else
#    define LG_SIZEOF_PTR_WIN 2
#  endif
#endif

/* sizeof(void *) == 2^LG_SIZEOF_PTR. */
#include <limits.h>
#ifdef _MSC_VER
#  define LG_SIZEOF_PTR LG_SIZEOF_PTR_WIN
#elif INTPTR_MAX == INT64_MAX
#  define LG_SIZEOF_PTR 3
#else
#  define LG_SIZEOF_PTR 2
#endif
```

We also supply our own config string in `jemalloc.c`.
Define this just after the `#include`s.
```c++
#define JE_MALLOC_CONF_BUFFER_SIZE 200;
char JE_MALLOC_CONF_BUFFER[JE_MALLOC_CONF_BUFFER_SIZE];
```
Then put this before `malloc_init()` in `static void jemalloc_constructor(void)`:
```c++
unsigned long long cpu_count = malloc_ncpus();
unsigned long long bgt_count = cpu_count / 32;
if (bgt_count == 0) {
    bgt_count = 1;
}
#ifdef DEBUG
	snprintf(JE_MALLOC_CONF_BUFFER, JE_MALLOC_CONF_BUFFER_SIZE, "junk:true,metadata_thp:always,oversize_threshold:0,dirty_decay_ms:10000,muzzy_decay_ms:10000,narenas:%llu,max_background_threads:%llu", cpu_count, bgt_count);
#else
	snprintf(JE_MALLOC_CONF_BUFFER, JE_MALLOC_CONF_BUFFER_SIZE, "metadata_thp:always,oversize_threshold:0,dirty_decay_ms:10000,muzzy_decay_ms:10000,narenas:%llu,max_background_threads:%llu", cpu_count, bgt_count);
#endif
je_malloc_conf = JE_MALLOC_CONF_BUFFER;
```

Almost no symbols are leaked due to `private_namespace.h`.
The `exported_symbols_check.py` script still found a few, so these lines need to be added to `private_namespace.h`:
```c++
// DuckDB: added these so we can pass "exported_symbols_check.py"
#define JE_MALLOC_CONF_BUFFER JEMALLOC_N(JE_MALLOC_CONF_BUFFER)
#define arena_name_get JEMALLOC_N(arena_name_get)
#define arena_name_set JEMALLOC_N(arena_name_set)
#define b0_alloc_tcache_stack JEMALLOC_N(b0_alloc_tcache_stack)
#define b0_dalloc_tcache_stack JEMALLOC_N(b0_dalloc_tcache_stack)
#define base_alloc_rtree JEMALLOC_N(base_alloc_rtree)
#define cache_bin_stack_use_thp JEMALLOC_N(cache_bin_stack_use_thp)
#define disabled_bin JEMALLOC_N(disabled_bin)
#define global_do_not_change_tcache_maxclass JEMALLOC_N(global_do_not_change_tcache_maxclass)
#define global_do_not_change_tcache_nbins JEMALLOC_N(global_do_not_change_tcache_nbins)
#define invalid_conf_abort JEMALLOC_N(invalid_conf_abort)
#define je_free_aligned_sized JEMALLOC_N(je_free_aligned_sized)
#define je_free_sized JEMALLOC_N(je_free_sized)
#define _malloc_thread_cleanup JEMALLOC_N(_malloc_thread_cleanup)
#define _malloc_tsd_cleanup_register JEMALLOC_N(_malloc_tsd_cleanup_register)
#define multi_setting_parse_next JEMALLOC_N(multi_setting_parse_next)
#define opt_calloc_madvise_threshold JEMALLOC_N(opt_calloc_madvise_threshold)
#define opt_debug_double_free_max_scan JEMALLOC_N(opt_debug_double_free_max_scan)
#define opt_malloc_conf_env_var JEMALLOC_N(opt_malloc_conf_env_var)
#define opt_malloc_conf_symlink JEMALLOC_N(opt_malloc_conf_symlink)
#define opt_prof_bt_max JEMALLOC_N(opt_prof_bt_max)
#define opt_prof_pid_namespace JEMALLOC_N(opt_prof_pid_namespace)
#define os_page JEMALLOC_N(os_page)
#define pa_shard_nactive JEMALLOC_N(pa_shard_nactive)
#define pa_shard_ndirty JEMALLOC_N(pa_shard_ndirty)
#define pa_shard_nmuzzy JEMALLOC_N(pa_shard_nmuzzy)
#define prof_sample_free_hook_get JEMALLOC_N(prof_sample_free_hook_get)
#define prof_sample_free_hook_set JEMALLOC_N(prof_sample_free_hook_set)
#define prof_sample_hook_get JEMALLOC_N(prof_sample_hook_get)
#define prof_sample_hook_set JEMALLOC_N(prof_sample_hook_set)
#define pthread_create_wrapper JEMALLOC_N(pthread_create_wrapper)
#define tcache_bin_ncached_max_read JEMALLOC_N(tcache_bin_ncached_max_read)
#define tcache_bins_ncached_max_write JEMALLOC_N(tcache_bins_ncached_max_write)
#define tcache_enabled_set JEMALLOC_N(tcache_enabled_set)
#define thread_tcache_max_set JEMALLOC_N(thread_tcache_max_set)
#define tsd_tls JEMALLOC_N(tsd_tls)
```