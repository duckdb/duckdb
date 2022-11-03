//#pragma once

//#include <cstdlib>
#include <cstdbool>
#include <cstdint>

#include <climits>

#ifdef _MSC_VER
#include "msvc_compat/strings.h"
#else
#include <strings.h>
#endif

namespace duckdb_jemalloc {

#define JEMALLOC_VERSION "5.3.0-0-g54eaed1d8b56b1aa528be3bdd1877e59c56fa90c"
#define JEMALLOC_VERSION_MAJOR 5
#define JEMALLOC_VERSION_MINOR 3
#define JEMALLOC_VERSION_BUGFIX 0
#define JEMALLOC_VERSION_NREV 0
#define JEMALLOC_VERSION_GID "54eaed1d8b56b1aa528be3bdd1877e59c56fa90c"
#define JEMALLOC_VERSION_GID_IDENT 54eaed1d8b56b1aa528be3bdd1877e59c56fa90c

#define MALLOCX_LG_ALIGN(la)	((int)(la))
#ifndef MALLOCX_ALIGN
#if LG_SIZEOF_PTR == 2
#  define MALLOCX_ALIGN(a)	((int)(ffs((int)(a))-1))
#else
#  define MALLOCX_ALIGN(a)						\
     ((int)(((size_t)(a) < (size_t)INT_MAX) ? ffs((int)(a))-1 :	\
     ffs((int)(((size_t)(a))>>32))+31))
#endif
#endif
#define MALLOCX_ZERO	((int)0x40)
/*
 * Bias tcache index bits so that 0 encodes "automatic tcache management", and 1
 * encodes MALLOCX_TCACHE_NONE.
 */
#ifndef MALLOCX_TCACHE
#define MALLOCX_TCACHE(tc)	((int)(((tc)+2) << 8))
#endif
#define MALLOCX_TCACHE_NONE	MALLOCX_TCACHE(-1)
/*
 * Bias arena index bits so that 0 encodes "use an automatically chosen arena".
 */
#define MALLOCX_ARENA(a)	((((int)(a))+1) << 20)

/*
 * Use as arena index in "arena.<i>.{purge,decay,dss}" and
 * "stats.arenas.<i>.*" mallctl interfaces to select all arenas.  This
 * definition is intentionally specified in raw decimal format to support
 * cpp-based string concatenation, e.g.
 *
 *   #define STRINGIFY_HELPER(x) #x
 *   #define STRINGIFY(x) STRINGIFY_HELPER(x)
 *
 *   mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", NULL, NULL, NULL,
 *       0);
 */
#define MALLCTL_ARENAS_ALL	4096
/*
 * Use as arena index in "stats.arenas.<i>.*" mallctl interfaces to select
 * destroyed arenas.
 */
#define MALLCTL_ARENAS_DESTROYED	4097

#if defined(__cplusplus) && defined(JEMALLOC_USE_CXX_THROW)
#  define JEMALLOC_CXX_THROW throw()
#else
#  define JEMALLOC_CXX_THROW
#endif

#define DLLEXPORT

#if defined(_MSC_VER)
#  define JEMALLOC_ATTR(s)
#  define JEMALLOC_ALIGNED(s) __declspec(align(s))
#  define JEMALLOC_ALLOC_SIZE(s)
#  define JEMALLOC_ALLOC_SIZE2(s1, s2)
#  ifndef JEMALLOC_EXPORT
#    ifdef DLLEXPORT
#      define JEMALLOC_EXPORT __declspec(dllexport)
#    else
#      define JEMALLOC_EXPORT __declspec(dllimport)
#    endif
#  endif
#  define JEMALLOC_FORMAT_ARG(i)
#  define JEMALLOC_FORMAT_PRINTF(s, i)
#  define JEMALLOC_FALLTHROUGH
#  define JEMALLOC_NOINLINE __declspec(noinline)
#  ifdef __cplusplus
#    define JEMALLOC_NOTHROW __declspec(nothrow)
#  else
#    define JEMALLOC_NOTHROW
#  endif
#  define JEMALLOC_SECTION(s) __declspec(allocate(s))
#  define JEMALLOC_RESTRICT_RETURN __declspec(restrict)
#  if _MSC_VER >= 1900 && !defined(__EDG__)
#    define JEMALLOC_ALLOCATOR __declspec(allocator)
#  else
#    define JEMALLOC_ALLOCATOR
#  endif
#  define JEMALLOC_COLD
#elif defined(JEMALLOC_HAVE_ATTR)
#  define JEMALLOC_ATTR(s) __attribute__((s))
#  define JEMALLOC_ALIGNED(s) JEMALLOC_ATTR(aligned(s))
#  ifdef JEMALLOC_HAVE_ATTR_ALLOC_SIZE
#    define JEMALLOC_ALLOC_SIZE(s) JEMALLOC_ATTR(alloc_size(s))
#    define JEMALLOC_ALLOC_SIZE2(s1, s2) JEMALLOC_ATTR(alloc_size(s1, s2))
#  else
#    define JEMALLOC_ALLOC_SIZE(s)
#    define JEMALLOC_ALLOC_SIZE2(s1, s2)
#  endif
#  ifndef JEMALLOC_EXPORT
#    define JEMALLOC_EXPORT JEMALLOC_ATTR(visibility("default"))
#  endif
#  ifdef JEMALLOC_HAVE_ATTR_FORMAT_ARG
#    define JEMALLOC_FORMAT_ARG(i) JEMALLOC_ATTR(__format_arg__(3))
#  else
#    define JEMALLOC_FORMAT_ARG(i)
#  endif
#  ifdef JEMALLOC_HAVE_ATTR_FORMAT_GNU_PRINTF
#    define JEMALLOC_FORMAT_PRINTF(s, i) JEMALLOC_ATTR(format(gnu_printf, s, i))
#  elif defined(JEMALLOC_HAVE_ATTR_FORMAT_PRINTF)
#    define JEMALLOC_FORMAT_PRINTF(s, i) JEMALLOC_ATTR(format(printf, s, i))
#  else
#    define JEMALLOC_FORMAT_PRINTF(s, i)
#  endif
#  ifdef JEMALLOC_HAVE_ATTR_FALLTHROUGH
#    define JEMALLOC_FALLTHROUGH JEMALLOC_ATTR(fallthrough)
#  else
#    define JEMALLOC_FALLTHROUGH
#  endif
#  define JEMALLOC_NOINLINE JEMALLOC_ATTR(noinline)
#  define JEMALLOC_NOTHROW JEMALLOC_ATTR(nothrow)
#  define JEMALLOC_SECTION(s) JEMALLOC_ATTR(section(s))
#  define JEMALLOC_RESTRICT_RETURN
#  define JEMALLOC_ALLOCATOR
#  ifdef JEMALLOC_HAVE_ATTR_COLD
#    define JEMALLOC_COLD JEMALLOC_ATTR(__cold__)
#  else
#    define JEMALLOC_COLD
#  endif
#else
#  define JEMALLOC_ATTR(s)
#  define JEMALLOC_ALIGNED(s)
#  define JEMALLOC_ALLOC_SIZE(s)
#  define JEMALLOC_ALLOC_SIZE2(s1, s2)
#  define JEMALLOC_EXPORT
#  define JEMALLOC_FORMAT_PRINTF(s, i)
#  define JEMALLOC_FALLTHROUGH
#  define JEMALLOC_NOINLINE
#  define JEMALLOC_NOTHROW
#  define JEMALLOC_SECTION(s)
#  define JEMALLOC_RESTRICT_RETURN
#  define JEMALLOC_ALLOCATOR
#  define JEMALLOC_COLD
#endif

#if (defined(__APPLE__) || defined(__FreeBSD__)) && !defined(JEMALLOC_NO_RENAME)
#  define JEMALLOC_SYS_NOTHROW
#else
#  define JEMALLOC_SYS_NOTHROW JEMALLOC_NOTHROW
#endif

} // namespace duckdb_jemalloc
