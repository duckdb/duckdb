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

Add this to `jemalloc.h`:
```c++
// DuckDB uses a 5s decay
#define DUCKDB_JEMALLOC_DECAY 5
```

We also supply our own config string in `jemalloc.c`.
Define this just after the `#include`s.
```c++
#define JE_MALLOC_CONF_BUFFER_SIZE 200
char JE_MALLOC_CONF_BUFFER[JE_MALLOC_CONF_BUFFER_SIZE];
```
This is what `jemalloc_constructor` in `jemalloc.c` should look like:
```c++
JEMALLOC_ATTR(constructor)
static void
jemalloc_constructor(void) {
	unsigned long long cpu_count = malloc_ncpus();
	unsigned long long bgt_count = cpu_count / 16;
	if (bgt_count == 0) {
		bgt_count = 1;
	}
	// decay is in ms
	unsigned long long decay = DUCKDB_JEMALLOC_DECAY * 1000;
#ifdef DEBUG
	snprintf(JE_MALLOC_CONF_BUFFER, JE_MALLOC_CONF_BUFFER_SIZE, "junk:true,oversize_threshold:268435456,dirty_decay_ms:%llu,muzzy_decay_ms:%llu,narenas:%llu,max_background_threads:%llu", decay, decay, cpu_count / 2, bgt_count);
#else
	snprintf(JE_MALLOC_CONF_BUFFER, JE_MALLOC_CONF_BUFFER_SIZE, "oversize_threshold:268435456,dirty_decay_ms:%llu,muzzy_decay_ms:%llu,narenas:%llu,max_background_threads:%llu", decay, decay, cpu_count / 2, bgt_count);
#endif
	je_malloc_conf = JE_MALLOC_CONF_BUFFER;
	malloc_init();
}
```

Make `strerror_r` portable using this hack in `malloc_io.c`, just above the `buferror` function:
```c++
// taken from https://ae1020.github.io/fixing-strerror_r-posix-debacle/
int strerror_fixed(int err, char *buf, size_t buflen) {
    assert(buflen != 0);

    buf[0] = (char)255;  // never valid in UTF-8 sequences
    int old_errno = errno;
    intptr_t r = (intptr_t)strerror_r(err, buf, buflen);
    int new_errno = errno;

    if (r == -1 || new_errno != old_errno) {
        //
        // errno was changed, so probably the return value is just -1 or
        // something else that doesn't provide info.
        //
        malloc_snprintf(buf, buflen, "errno %d in strerror_r call", new_errno);
    }
    else if (r == 0) {
        //
        // The GNU version always succeds and should never return 0 (NULL).
        //
        // "The XSI-compliant strerror_r() function returns 0 on success.
        // On error, a (positive) error number is returned (since glibc
        // 2.13), or -1 is returned and errno is set to indicate the error
        // (glibc versions before 2.13)."
        //
        // Documentation isn't clear on whether the buffer is terminated if
        // the message is too long, or ERANGE always returned.  Terminate.
        //
        buf[buflen - 1] = '\0';
    }
    else if (r == EINVAL) {  // documented result from XSI strerror_r
        malloc_snprintf(buf, buflen, "bad errno %d for strerror_r()", err);
    }
    else if (r == ERANGE) {  // documented result from XSI strerror_r
        malloc_snprintf(buf, buflen, "bad buflen for errno %d", err);
    }
    else if (r == (intptr_t)buf) {
        //
        // The GNU version gives us our error back as a pointer if it
        // filled the buffer successfully.  Sanity check that.
        //
        if (buf[0] == (char)255) {
            assert(false);
            strncpy(buf, "strerror_r didn't update buffer", buflen);
        }
    }
    else if (r < 256) {  // extremely unlikely to be string buffer pointer
        assert(false);
        strncpy(buf, "Unknown XSI strerror_r error result code", buflen);
    }
    else {
        // The GNU version never fails, but may return an immutable string
        // instead of filling the buffer. Unknown errors get an
        // "unknown error" message.  The result is always null terminated.
        //
        // (This is the risky part, if `r` is not a valid pointer but some
        // weird large int return result from XSI strerror_r.)
        //
        strncpy(buf, (const char*)r, buflen);
    }
	return 0;
}
```

Edit the following in `pages.c`:
```c++
// explicitly initialize this buffer to prevent reading uninitialized memory if the file is somehow empty
// 0 is the default setting for linux if it hasn't been changed so that's what we initialize to
char buf[1] = {'0'};
// in this function
static bool
os_overcommits_proc(void)
```

Modify this function to only print in DEBUG mode in `malloc_io.c`.
```c++
void
malloc_write(const char *s) {
#ifdef DEBUG
	if (je_malloc_message != NULL) {
		je_malloc_message(NULL, s);
	} else {
		wrtmessage(NULL, s);
	}
#endif
}
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
#define batcher_pop_begin JEMALLOC_N(batcher_pop_begin)
#define batcher_pop_get_pushes JEMALLOC_N(batcher_pop_get_pushes)
#define batcher_postfork_child JEMALLOC_N(batcher_postfork_child)
#define batcher_postfork_parent JEMALLOC_N(batcher_postfork_parent)
#define batcher_prefork JEMALLOC_N(batcher_prefork)
#define batcher_push_begin JEMALLOC_N(batcher_push_begin)
#define bin_info_nbatched_bins JEMALLOC_N(bin_info_nbatched_bins)
#define bin_info_nbatched_sizes JEMALLOC_N(bin_info_nbatched_sizes)
#define bin_info_nunbatched_bins JEMALLOC_N(bin_info_nunbatched_bins)
#define opt_bin_info_max_batched_size JEMALLOC_N(opt_bin_info_max_batched_size)
#define opt_bin_info_remote_free_max JEMALLOC_N(opt_bin_info_remote_free_max)
#define opt_bin_info_remote_free_max_batch JEMALLOC_N(opt_bin_info_remote_free_max_batch)
```
