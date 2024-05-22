/* include/jemalloc/jemalloc_defs.h.  Generated from jemalloc_defs.h.in by configure.  */
/* Defined if __attribute__((...)) syntax is supported. */
#define JEMALLOC_HAVE_ATTR 

/* Defined if alloc_size attribute is supported. */
#define JEMALLOC_HAVE_ATTR_ALLOC_SIZE 

/* Defined if format_arg(...) attribute is supported. */
#define JEMALLOC_HAVE_ATTR_FORMAT_ARG 

/* Defined if format(gnu_printf, ...) attribute is supported. */
/* #undef JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF */

/* Defined if format(printf, ...) attribute is supported. */
#define JEMALLOC_HAVE_ATTR_FORMAT_PRINTF 

/* Defined if fallthrough attribute is supported. */
#define JEMALLOC_HAVE_ATTR_FALLTHROUGH 

/* Defined if cold attribute is supported. */
#define JEMALLOC_HAVE_ATTR_COLD 

/* Defined if deprecated attribute is supported. */
/* #undef JEMALLOC_HAVE_ATTR_DEPRECATED */

/*
 * Define overrides for non-standard allocator-related functions if they are
 * present on the system.
 */
/* #undef JEMALLOC_OVERRIDE_MEMALIGN */
#define JEMALLOC_OVERRIDE_VALLOC 
/* #undef JEMALLOC_OVERRIDE_PVALLOC */

/*
 * At least Linux omits the "const" in:
 *
 *   size_t malloc_usable_size(const void *ptr);
 *
 * Match the operating system's prototype.
 */
#define JEMALLOC_USABLE_SIZE_CONST const

/*
 * If defined, specify throw() for the public function prototypes when compiling
 * with C++.  The only justification for this is to match the prototypes that
 * glibc defines.
 */
/* #undef JEMALLOC_USE_CXX_THROW */

#ifdef _MSC_VER
#  ifdef _WIN64
#    define LG_SIZEOF_PTR_WIN 3
#  else
#    define LG_SIZEOF_PTR_WIN 2
#  endif
#endif

/* sizeof(void *) == 2^LG_SIZEOF_PTR. */
#include "limits.h"
#ifdef _MSC_VER
#  define LG_SIZEOF_PTR LG_SIZEOF_PTR_WIN
#elif INTPTR_MAX == INT64_MAX
#  define LG_SIZEOF_PTR 3
#else
#  define LG_SIZEOF_PTR 2
#endif
