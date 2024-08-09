/*
 * By default application code must explicitly refer to mangled symbol names,
 * so that it is possible to use jemalloc in conjunction with another allocator
 * in the same application.  Define JEMALLOC_MANGLE in order to cause automatic
 * name mangling that matches the API prefixing that happened as a result of
 * --with-mangling and/or --with-jemalloc-prefix configuration settings.
 */
#ifdef JEMALLOC_MANGLE
#  ifndef JEMALLOC_NO_DEMANGLE
#    define JEMALLOC_NO_DEMANGLE
#  endif
#  define aligned_alloc jet_aligned_alloc
#  define calloc jet_calloc
#  define dallocx jet_dallocx
#  define free jet_free
#  define free_sized jet_free_sized
#  define free_aligned_sized jet_free_aligned_sized
#  define mallctl jet_mallctl
#  define mallctlbymib jet_mallctlbymib
#  define mallctlnametomib jet_mallctlnametomib
#  define malloc jet_malloc
#  define malloc_conf jet_malloc_conf
#  define malloc_conf_2_conf_harder jet_malloc_conf_2_conf_harder
#  define malloc_message jet_malloc_message
#  define malloc_stats_print jet_malloc_stats_print
#  define malloc_usable_size jet_malloc_usable_size
#  define mallocx jet_mallocx
#  define smallocx_a25b9b8ba91881964be3083db349991bbbbf1661 jet_smallocx_a25b9b8ba91881964be3083db349991bbbbf1661
#  define nallocx jet_nallocx
#  define posix_memalign jet_posix_memalign
#  define rallocx jet_rallocx
#  define realloc jet_realloc
#  define sallocx jet_sallocx
#  define sdallocx jet_sdallocx
#  define xallocx jet_xallocx
#  define valloc jet_valloc
#  define malloc_size jet_malloc_size
#endif

/*
 * The jet_* macros can be used as stable alternative names for the
 * public jemalloc API if JEMALLOC_NO_DEMANGLE is defined.  This is primarily
 * meant for use in jemalloc itself, but it can be used by application code to
 * provide isolation from the name mangling specified via --with-mangling
 * and/or --with-jemalloc-prefix.
 */
#ifndef JEMALLOC_NO_DEMANGLE
#  undef jet_aligned_alloc
#  undef jet_calloc
#  undef jet_dallocx
#  undef jet_free
#  undef jet_free_sized
#  undef jet_free_aligned_sized
#  undef jet_mallctl
#  undef jet_mallctlbymib
#  undef jet_mallctlnametomib
#  undef jet_malloc
#  undef jet_malloc_conf
#  undef jet_malloc_conf_2_conf_harder
#  undef jet_malloc_message
#  undef jet_malloc_stats_print
#  undef jet_malloc_usable_size
#  undef jet_mallocx
#  undef jet_smallocx_a25b9b8ba91881964be3083db349991bbbbf1661
#  undef jet_nallocx
#  undef jet_posix_memalign
#  undef jet_rallocx
#  undef jet_realloc
#  undef jet_sallocx
#  undef jet_sdallocx
#  undef jet_xallocx
#  undef jet_valloc
#  undef jet_malloc_size
#endif
