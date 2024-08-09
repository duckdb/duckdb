/*
 * Name mangling for public symbols is controlled by --with-mangling and
 * --with-jemalloc-prefix.  With default settings the je_ prefix is stripped by
 * these macro definitions.
 */
#ifndef JEMALLOC_NO_RENAME
#  define je_aligned_alloc duckdb_je_aligned_alloc
#  define je_calloc duckdb_je_calloc
#  define je_dallocx duckdb_je_dallocx
#  define je_free duckdb_je_free
#  define je_free_sized duckdb_je_free_sized
#  define je_free_aligned_sized duckdb_je_free_aligned_sized
#  define je_mallctl duckdb_je_mallctl
#  define je_mallctlbymib duckdb_je_mallctlbymib
#  define je_mallctlnametomib duckdb_je_mallctlnametomib
#  define je_malloc duckdb_je_malloc
#  define je_malloc_conf duckdb_je_malloc_conf
#  define je_malloc_conf_2_conf_harder duckdb_je_malloc_conf_2_conf_harder
#  define je_malloc_message duckdb_je_malloc_message
#  define je_malloc_stats_print duckdb_je_malloc_stats_print
#  define je_malloc_usable_size duckdb_je_malloc_usable_size
#  define je_mallocx duckdb_je_mallocx
#  define je_smallocx_a25b9b8ba91881964be3083db349991bbbbf1661 duckdb_je_smallocx_a25b9b8ba91881964be3083db349991bbbbf1661
#  define je_nallocx duckdb_je_nallocx
#  define je_posix_memalign duckdb_je_posix_memalign
#  define je_rallocx duckdb_je_rallocx
#  define je_realloc duckdb_je_realloc
#  define je_sallocx duckdb_je_sallocx
#  define je_sdallocx duckdb_je_sdallocx
#  define je_xallocx duckdb_je_xallocx
#  define je_valloc duckdb_je_valloc
#  define je_malloc_size duckdb_je_malloc_size
#endif
