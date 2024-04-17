# Updating jemalloc

Clone [this](https://github.com/jemalloc/jemalloc).

For convenience:
```sh
export DUCKDB_DIR=<duckdb_dir>
```

Copy jemalloc source files:
```sh
cd <jemalloc_dir>
./configure --with-jemalloc-prefix="duckdb_je_" --with-private-namespace="duckdb_"
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
unsigned long long bgt_count = cpu_count / 8;
if (bgt_count == 0) {
    bgt_count = 1;
}
snprintf(JE_MALLOC_CONF_BUFFER, JE_MALLOC_CONF_BUFFER_SIZE, "narenas:%llu,dirty_decay_ms:10000,muzzy_decay_ms:10000,max_background_threads:%llu", cpu_count, bgt_count);
je_malloc_conf = JE_MALLOC_CONF_BUFFER;
```