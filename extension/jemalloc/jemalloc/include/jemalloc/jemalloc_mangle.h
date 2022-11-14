/*
 * By default application code must explicitly refer to mangled symbol names,
 * so that it is possible to use jemalloc in conjunction with another allocator
 * in the same application.  Define JEMALLOC_MANGLE in order to cause automatic
 * name mangling that matches the API prefixing that happened as a result of
 * --with-mangling and/or --with-jemalloc-prefix configuration settings.
 */

namespace duckdb_jemalloc {

#ifdef JEMALLOC_MANGLE
#  ifndef JEMALLOC_NO_DEMANGLE
#    define JEMALLOC_NO_DEMANGLE
#  endif
#  define aligned_alloc je_aligned_alloc
#  define calloc je_calloc
#  define dallocx je_dallocx
#  define free je_free
#  define mallctl je_mallctl
#  define mallctlbymib je_mallctlbymib
#  define mallctlnametomib je_mallctlnametomib
#  define malloc je_malloc
#  define malloc_conf je_malloc_conf
#  define malloc_conf_2_conf_harder je_malloc_conf_2_conf_harder
#  define malloc_message je_malloc_message
#  define malloc_stats_print je_malloc_stats_print
#  define malloc_usable_size je_malloc_usable_size
#  define mallocx je_mallocx
#  define smallocx_54eaed1d8b56b1aa528be3bdd1877e59c56fa90c je_smallocx_54eaed1d8b56b1aa528be3bdd1877e59c56fa90c
#  define nallocx je_nallocx
#  define posix_memalign je_posix_memalign
#  define rallocx je_rallocx
#  define realloc je_realloc
#  define sallocx je_sallocx
#  define sdallocx je_sdallocx
#  define xallocx je_xallocx
#  define valloc je_valloc
#  define malloc_size je_malloc_size
#endif

/*
 * The je_* macros can be used as stable alternative names for the
 * public jemalloc API if JEMALLOC_NO_DEMANGLE is defined.  This is primarily
 * meant for use in jemalloc itself, but it can be used by application code to
 * provide isolation from the name mangling specified via --with-mangling
 * and/or --with-jemalloc-prefix.
 */
#ifndef JEMALLOC_NO_DEMANGLE
#  undef je_aligned_alloc
#  undef je_calloc
#  undef je_dallocx
#  undef je_free
#  undef je_mallctl
#  undef je_mallctlbymib
#  undef je_mallctlnametomib
#  undef je_malloc
#  undef je_malloc_conf
#  undef je_malloc_conf_2_conf_harder
#  undef je_malloc_message
#  undef je_malloc_stats_print
#  undef je_malloc_usable_size
#  undef je_mallocx
#  undef je_smallocx_54eaed1d8b56b1aa528be3bdd1877e59c56fa90c
#  undef je_nallocx
#  undef je_posix_memalign
#  undef je_rallocx
#  undef je_realloc
#  undef je_sallocx
#  undef je_sdallocx
#  undef je_xallocx
#  undef je_valloc
#  undef je_malloc_size
#endif

} // namespace duckdb_jemalloc
