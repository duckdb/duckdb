#ifndef JEMALLOC_H_
#define JEMALLOC_H_
#pragma GCC system_header
#ifdef __cplusplus
extern "C" {
#endif

// DuckDB uses a 10s decay
#define DUCKDB_DECAY_DELAY 10

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
/* #undef JEMALLOC_OVERRIDE_VALLOC */
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
#include <limits.h>
#ifdef _MSC_VER
#  define LG_SIZEOF_PTR LG_SIZEOF_PTR_WIN
#elif INTPTR_MAX == INT64_MAX
#  define LG_SIZEOF_PTR 3
#else
#  define LG_SIZEOF_PTR 2
#endif

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
#  define je_smallocx_fa451de17fff73cc03c31ec8cd817d62927d1ff9 duckdb_je_smallocx_fa451de17fff73cc03c31ec8cd817d62927d1ff9
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

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <limits.h>
#include <strings.h>

#define JEMALLOC_VERSION "5.3.0-172-gfa451de17fff73cc03c31ec8cd817d62927d1ff9"
#define JEMALLOC_VERSION_MAJOR 5
#define JEMALLOC_VERSION_MINOR 3
#define JEMALLOC_VERSION_BUGFIX 0
#define JEMALLOC_VERSION_NREV 172
#define JEMALLOC_VERSION_GID "fa451de17fff73cc03c31ec8cd817d62927d1ff9"
#define JEMALLOC_VERSION_GID_IDENT fa451de17fff73cc03c31ec8cd817d62927d1ff9

#define MALLOCX_LG_ALIGN(la)	((int)(la))
#if LG_SIZEOF_PTR == 2
#  define MALLOCX_ALIGN(a)	((int)(ffs((int)(a))-1))
#else
#  define MALLOCX_ALIGN(a)						\
     ((int)(((size_t)(a) < (size_t)INT_MAX) ? ffs((int)(a))-1 :	\
     ffs((int)(((size_t)(a))>>32))+31))
#endif
#define MALLOCX_ZERO	((int)0x40)
/*
 * Bias tcache index bits so that 0 encodes "automatic tcache management", and 1
 * encodes MALLOCX_TCACHE_NONE.
 */
#define MALLOCX_TCACHE(tc)	((int)(((tc)+2) << 8))
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
#  define JEMALLOC_WARN_ON_USAGE(warning_message)
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
#  ifdef JEMALLOC_HAVE_ATTR_DEPRECATED
#    define JEMALLOC_WARN_ON_USAGE(warning_message) JEMALLOC_ATTR(deprecated(warning_message))
#  else
#    define JEMALLOC_WARN_ON_USAGE(warning_message)
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
#  define JEMALLOC_WARN_ON_USAGE(warning_message)
#endif

#if (defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || (defined(__linux__) && !defined(__GLIBC__))) && !defined(JEMALLOC_NO_RENAME)
#  define JEMALLOC_SYS_NOTHROW
#else
#  define JEMALLOC_SYS_NOTHROW JEMALLOC_NOTHROW
#endif

/*
 * The je_ prefix on the following public symbol declarations is an artifact
 * of namespace management, and should be omitted in application code unless
 * JEMALLOC_NO_DEMANGLE is defined (see jemalloc_mangle.h).
 */
extern JEMALLOC_EXPORT const char	*je_malloc_conf;
extern JEMALLOC_EXPORT const char	*je_malloc_conf_2_conf_harder;
extern JEMALLOC_EXPORT void		(*je_malloc_message)(void *cbopaque,
    const char *s);

JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_malloc(size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE(1);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_calloc(size_t num, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE2(1, 2);
JEMALLOC_EXPORT int JEMALLOC_SYS_NOTHROW je_posix_memalign(
    void **memptr, size_t alignment, size_t size) JEMALLOC_CXX_THROW
    JEMALLOC_ATTR(nonnull(1));
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_aligned_alloc(size_t alignment,
    size_t size) JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc)
    JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_realloc(void *ptr, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW	je_free(void *ptr)
    JEMALLOC_CXX_THROW;
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_free_sized(void *ptr, size_t size);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_free_aligned_sized(
    void *ptr, size_t alignment, size_t size);

JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_mallocx(size_t size, int flags)
    JEMALLOC_ATTR(malloc) JEMALLOC_ALLOC_SIZE(1);
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_NOTHROW	*je_rallocx(void *ptr, size_t size,
    int flags) JEMALLOC_ALLOC_SIZE(2);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_xallocx(void *ptr, size_t size,
    size_t extra, int flags);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_sallocx(const void *ptr,
    int flags) JEMALLOC_ATTR(pure);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_dallocx(void *ptr, int flags);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_sdallocx(void *ptr, size_t size,
    int flags);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_nallocx(size_t size, int flags)
    JEMALLOC_ATTR(pure);

JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctl(const char *name,
    void *oldp, size_t *oldlenp, void *newp, size_t newlen);
JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctlnametomib(const char *name,
    size_t *mibp, size_t *miblenp);
JEMALLOC_EXPORT int JEMALLOC_NOTHROW	je_mallctlbymib(const size_t *mib,
    size_t miblen, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
JEMALLOC_EXPORT void JEMALLOC_NOTHROW	je_malloc_stats_print(
    void (*write_cb)(void *, const char *), void *je_cbopaque,
    const char *opts);
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_malloc_usable_size(
    JEMALLOC_USABLE_SIZE_CONST void *ptr) JEMALLOC_CXX_THROW;
#ifdef JEMALLOC_HAVE_MALLOC_SIZE
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW	je_malloc_size(
    const void *ptr);
#endif

#ifdef JEMALLOC_OVERRIDE_MEMALIGN
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_memalign(size_t alignment, size_t size)
    JEMALLOC_CXX_THROW JEMALLOC_ATTR(malloc);
#endif

#ifdef JEMALLOC_OVERRIDE_VALLOC
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_valloc(size_t size) JEMALLOC_CXX_THROW
    JEMALLOC_ATTR(malloc);
#endif

#ifdef JEMALLOC_OVERRIDE_PVALLOC
JEMALLOC_EXPORT JEMALLOC_ALLOCATOR JEMALLOC_RESTRICT_RETURN
    void JEMALLOC_SYS_NOTHROW	*je_pvalloc(size_t size) JEMALLOC_CXX_THROW
    JEMALLOC_ATTR(malloc);
#endif

typedef struct extent_hooks_s extent_hooks_t;

/*
 * void *
 * extent_alloc(extent_hooks_t *extent_hooks, void *new_addr, size_t size,
 *     size_t alignment, bool *zero, bool *commit, unsigned arena_ind);
 */
typedef void *(extent_alloc_t)(extent_hooks_t *, void *, size_t, size_t, bool *,
    bool *, unsigned);

/*
 * bool
 * extent_dalloc(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     bool committed, unsigned arena_ind);
 */
typedef bool (extent_dalloc_t)(extent_hooks_t *, void *, size_t, bool,
    unsigned);

/*
 * void
 * extent_destroy(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     bool committed, unsigned arena_ind);
 */
typedef void (extent_destroy_t)(extent_hooks_t *, void *, size_t, bool,
    unsigned);

/*
 * bool
 * extent_commit(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     size_t offset, size_t length, unsigned arena_ind);
 */
typedef bool (extent_commit_t)(extent_hooks_t *, void *, size_t, size_t, size_t,
    unsigned);

/*
 * bool
 * extent_decommit(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     size_t offset, size_t length, unsigned arena_ind);
 */
typedef bool (extent_decommit_t)(extent_hooks_t *, void *, size_t, size_t,
    size_t, unsigned);

/*
 * bool
 * extent_purge(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     size_t offset, size_t length, unsigned arena_ind);
 */
typedef bool (extent_purge_t)(extent_hooks_t *, void *, size_t, size_t, size_t,
    unsigned);

/*
 * bool
 * extent_split(extent_hooks_t *extent_hooks, void *addr, size_t size,
 *     size_t size_a, size_t size_b, bool committed, unsigned arena_ind);
 */
typedef bool (extent_split_t)(extent_hooks_t *, void *, size_t, size_t, size_t,
    bool, unsigned);

/*
 * bool
 * extent_merge(extent_hooks_t *extent_hooks, void *addr_a, size_t size_a,
 *     void *addr_b, size_t size_b, bool committed, unsigned arena_ind);
 */
typedef bool (extent_merge_t)(extent_hooks_t *, void *, size_t, void *, size_t,
    bool, unsigned);

struct extent_hooks_s {
	extent_alloc_t		*alloc;
	extent_dalloc_t		*dalloc;
	extent_destroy_t	*destroy;
	extent_commit_t		*commit;
	extent_decommit_t	*decommit;
	extent_purge_t		*purge_lazy;
	extent_purge_t		*purge_forced;
	extent_split_t		*split;
	extent_merge_t		*merge;
};

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
#  define smallocx_fa451de17fff73cc03c31ec8cd817d62927d1ff9 je_smallocx_fa451de17fff73cc03c31ec8cd817d62927d1ff9
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
#  undef je_smallocx_fa451de17fff73cc03c31ec8cd817d62927d1ff9
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

#ifdef __cplusplus
}
#endif
#endif /* JEMALLOC_H_ */
