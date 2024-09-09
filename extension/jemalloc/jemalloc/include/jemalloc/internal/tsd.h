#ifndef JEMALLOC_INTERNAL_TSD_H
#define JEMALLOC_INTERNAL_TSD_H

/*
 * We put the platform-specific data declarations and inlines into their own
 * header files to avoid cluttering this file.  They define tsd_boot0,
 * tsd_boot1, tsd_boot, tsd_booted_get, tsd_get_allocates, tsd_get, and tsd_set.
 */
#ifdef JEMALLOC_MALLOC_THREAD_CLEANUP
#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/tsd_malloc_thread_cleanup.h"
#elif (defined(JEMALLOC_TLS))
#include "jemalloc/internal/tsd_tls.h"
#elif (defined(_WIN32))
#include "jemalloc/internal/tsd_win.h"
#else
#include "jemalloc/internal/tsd_generic.h"
#endif

/*
 * tsd_foop_get_unsafe(tsd) returns a pointer to the thread-local instance of
 * foo.  This omits some safety checks, and so can be used during tsd
 * initialization and cleanup.
 */
#define O(n, t, nt)							\
JEMALLOC_ALWAYS_INLINE t *						\
tsd_##n##p_get_unsafe(tsd_t *tsd) {					\
	return &tsd->TSD_MANGLE(n);					\
}
TSD_DATA_SLOW
TSD_DATA_FAST
TSD_DATA_SLOWER
#undef O

/* tsd_foop_get(tsd) returns a pointer to the thread-local instance of foo. */
#define O(n, t, nt)							\
JEMALLOC_ALWAYS_INLINE t *						\
tsd_##n##p_get(tsd_t *tsd) {						\
	/*								\
	 * Because the state might change asynchronously if it's	\
	 * nominal, we need to make sure that we only read it once.	\
	 */								\
	uint8_t state = tsd_state_get(tsd);				\
	assert(state == tsd_state_nominal ||				\
	    state == tsd_state_nominal_slow ||				\
	    state == tsd_state_nominal_recompute ||			\
	    state == tsd_state_reincarnated ||				\
	    state == tsd_state_minimal_initialized);			\
	return tsd_##n##p_get_unsafe(tsd);				\
}
TSD_DATA_SLOW
TSD_DATA_FAST
TSD_DATA_SLOWER
#undef O

/*
 * tsdn_foop_get(tsdn) returns either the thread-local instance of foo (if tsdn
 * isn't NULL), or NULL (if tsdn is NULL), cast to the nullable pointer type.
 */
#define O(n, t, nt)							\
JEMALLOC_ALWAYS_INLINE nt *						\
tsdn_##n##p_get(tsdn_t *tsdn) {						\
	if (tsdn_null(tsdn)) {						\
		return NULL;						\
	}								\
	tsd_t *tsd = tsdn_tsd(tsdn);					\
	return (nt *)tsd_##n##p_get(tsd);				\
}
TSD_DATA_SLOW
TSD_DATA_FAST
TSD_DATA_SLOWER
#undef O

/* tsd_foo_get(tsd) returns the value of the thread-local instance of foo. */
#define O(n, t, nt)							\
JEMALLOC_ALWAYS_INLINE t						\
tsd_##n##_get(tsd_t *tsd) {						\
	return *tsd_##n##p_get(tsd);					\
}
TSD_DATA_SLOW
TSD_DATA_FAST
TSD_DATA_SLOWER
#undef O

/* tsd_foo_set(tsd, val) updates the thread-local instance of foo to be val. */
#define O(n, t, nt)							\
JEMALLOC_ALWAYS_INLINE void						\
tsd_##n##_set(tsd_t *tsd, t val) {					\
	assert(tsd_state_get(tsd) != tsd_state_reincarnated &&		\
	    tsd_state_get(tsd) != tsd_state_minimal_initialized);	\
	*tsd_##n##p_get(tsd) = val;					\
}
TSD_DATA_SLOW
TSD_DATA_FAST
TSD_DATA_SLOWER
#undef O

JEMALLOC_ALWAYS_INLINE void
tsd_assert_fast(tsd_t *tsd) {
	/*
	 * Note that our fastness assertion does *not* include global slowness
	 * counters; it's not in general possible to ensure that they won't
	 * change asynchronously from underneath us.
	 */
	assert(!malloc_slow && tsd_tcache_enabled_get(tsd) &&
	    tsd_reentrancy_level_get(tsd) == 0);
}

JEMALLOC_ALWAYS_INLINE bool
tsd_fast(tsd_t *tsd) {
	bool fast = (tsd_state_get(tsd) == tsd_state_nominal);
	if (fast) {
		tsd_assert_fast(tsd);
	}

	return fast;
}

JEMALLOC_ALWAYS_INLINE tsd_t *
tsd_fetch_impl(bool init, bool minimal) {
	tsd_t *tsd = tsd_get(init);

	if (!init && tsd_get_allocates() && tsd == NULL) {
		return NULL;
	}
	assert(tsd != NULL);

	if (unlikely(tsd_state_get(tsd) != tsd_state_nominal)) {
		return tsd_fetch_slow(tsd, minimal);
	}
	assert(tsd_fast(tsd));
	tsd_assert_fast(tsd);

	return tsd;
}

/* Get a minimal TSD that requires no cleanup.  See comments in free(). */
JEMALLOC_ALWAYS_INLINE tsd_t *
tsd_fetch_min(void) {
	return tsd_fetch_impl(true, true);
}

/* For internal background threads use only. */
JEMALLOC_ALWAYS_INLINE tsd_t *
tsd_internal_fetch(void) {
	tsd_t *tsd = tsd_fetch_min();
	/* Use reincarnated state to prevent full initialization. */
	tsd_state_set(tsd, tsd_state_reincarnated);

	return tsd;
}

JEMALLOC_ALWAYS_INLINE tsd_t *
tsd_fetch(void) {
	return tsd_fetch_impl(true, false);
}

static inline bool
tsd_nominal(tsd_t *tsd) {
	bool nominal = tsd_state_get(tsd) <= tsd_state_nominal_max;
	assert(nominal || tsd_reentrancy_level_get(tsd) > 0);

	return nominal;
}

JEMALLOC_ALWAYS_INLINE tsdn_t *
tsdn_fetch(void) {
	if (!tsd_booted_get()) {
		return NULL;
	}

	return tsd_tsdn(tsd_fetch_impl(false, false));
}

JEMALLOC_ALWAYS_INLINE rtree_ctx_t *
tsd_rtree_ctx(tsd_t *tsd) {
	return tsd_rtree_ctxp_get(tsd);
}

JEMALLOC_ALWAYS_INLINE rtree_ctx_t *
tsdn_rtree_ctx(tsdn_t *tsdn, rtree_ctx_t *fallback) {
	/*
	 * If tsd cannot be accessed, initialize the fallback rtree_ctx and
	 * return a pointer to it.
	 */
	if (unlikely(tsdn_null(tsdn))) {
		rtree_ctx_data_init(fallback);
		return fallback;
	}
	return tsd_rtree_ctx(tsdn_tsd(tsdn));
}

static inline bool
tsd_state_nocleanup(tsd_t *tsd) {
	return tsd_state_get(tsd) == tsd_state_reincarnated ||
	    tsd_state_get(tsd) == tsd_state_minimal_initialized;
}

/*
 * These "raw" tsd reentrancy functions don't have any debug checking to make
 * sure that we're not touching arena 0.  Better is to call pre_reentrancy and
 * post_reentrancy if this is possible.
 */
static inline void
tsd_pre_reentrancy_raw(tsd_t *tsd) {
	bool fast = tsd_fast(tsd);
	assert(tsd_reentrancy_level_get(tsd) < INT8_MAX);
	++*tsd_reentrancy_levelp_get(tsd);
	if (fast) {
		/* Prepare slow path for reentrancy. */
		tsd_slow_update(tsd);
		assert(tsd_state_get(tsd) == tsd_state_nominal_slow);
	}
}

static inline void
tsd_post_reentrancy_raw(tsd_t *tsd) {
	int8_t *reentrancy_level = tsd_reentrancy_levelp_get(tsd);
	assert(*reentrancy_level > 0);
	if (--*reentrancy_level == 0) {
		tsd_slow_update(tsd);
	}
}

#endif /* JEMALLOC_INTERNAL_TSD_H */
