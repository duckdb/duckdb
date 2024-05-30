#ifdef JEMALLOC_INTERNAL_TSD_INTERNALS_H
#error This file should be included only once, by one of tsd_malloc_thread_cleanup.h, tsd_tls.h, tsd_generic.h, or tsd_win.h
#endif
#define JEMALLOC_INTERNAL_TSD_INTERNALS_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/activity_callback.h"
#include "jemalloc/internal/arena_types.h"
#include "jemalloc/internal/assert.h"
#include "jemalloc/internal/bin_types.h"
#include "jemalloc/internal/jemalloc_internal_externs.h"
#include "jemalloc/internal/peak.h"
#include "jemalloc/internal/prof_types.h"
#include "jemalloc/internal/ql.h"
#include "jemalloc/internal/rtree_tsd.h"
#include "jemalloc/internal/tcache_structs.h"
#include "jemalloc/internal/tcache_types.h"
#include "jemalloc/internal/tsd_types.h"
#include "jemalloc/internal/util.h"
#include "jemalloc/internal/witness.h"

/*
 * Thread-Specific-Data layout
 *
 * At least some thread-local data gets touched on the fast-path of almost all
 * malloc operations.  But much of it is only necessary down slow-paths, or
 * testing.  We want to colocate the fast-path data so that it can live on the
 * same cacheline if possible.  So we define three tiers of hotness:
 * TSD_DATA_FAST: Touched on the alloc/dalloc fast paths.
 * TSD_DATA_SLOW: Touched down slow paths.  "Slow" here is sort of general;
 *     there are "semi-slow" paths like "not a sized deallocation, but can still
 *     live in the tcache".  We'll want to keep these closer to the fast-path
 *     data.
 * TSD_DATA_SLOWER: Only touched in test or debug modes, or not touched at all.
 *
 * An additional concern is that the larger tcache bins won't be used (we have a
 * bin per size class, but by default only cache relatively small objects).  So
 * the earlier bins are in the TSD_DATA_FAST tier, but the later ones are in the
 * TSD_DATA_SLOWER tier.
 *
 * As a result of all this, we put the slow data first, then the fast data, then
 * the slower data, while keeping the tcache as the last element of the fast
 * data (so that the fast -> slower transition happens midway through the
 * tcache).  While we don't yet play alignment tricks to guarantee it, this
 * increases our odds of getting some cache/page locality on fast paths.
 */

#ifdef JEMALLOC_JET
typedef void (*test_callback_t)(int *);
#  define MALLOC_TSD_TEST_DATA_INIT 0x72b65c10
#  define MALLOC_TEST_TSD \
    O(test_data,		int,			int)		\
    O(test_callback,		test_callback_t,	int)
#  define MALLOC_TEST_TSD_INITIALIZER , MALLOC_TSD_TEST_DATA_INIT, NULL
#else
#  define MALLOC_TEST_TSD
#  define MALLOC_TEST_TSD_INITIALIZER
#endif

typedef ql_elm(tsd_t) tsd_link_t;

/*  O(name,			type,			nullable type) */
#define TSD_DATA_SLOW							\
    O(tcache_enabled,		bool,			bool)		\
    O(reentrancy_level,		int8_t,			int8_t)		\
    O(min_init_state_nfetched,		uint8_t,	uint8_t)	\
    O(thread_allocated_last_event,	uint64_t,	uint64_t)	\
    O(thread_allocated_next_event,	uint64_t,	uint64_t)	\
    O(thread_deallocated_last_event,	uint64_t,	uint64_t)	\
    O(thread_deallocated_next_event,	uint64_t,	uint64_t)	\
    O(tcache_gc_event_wait,	uint64_t,		uint64_t)	\
    O(tcache_gc_dalloc_event_wait,	uint64_t,	uint64_t)	\
    O(prof_sample_event_wait,	uint64_t,		uint64_t)	\
    O(prof_sample_last_event,	uint64_t,		uint64_t)	\
    O(stats_interval_event_wait,	uint64_t,	uint64_t)	\
    O(stats_interval_last_event,	uint64_t,	uint64_t)	\
    O(peak_alloc_event_wait,	uint64_t,		uint64_t)	\
    O(peak_dalloc_event_wait,	uint64_t,	uint64_t)		\
    O(prof_tdata,		prof_tdata_t *,		prof_tdata_t *)	\
    O(prng_state,		uint64_t,		uint64_t)	\
    O(san_extents_until_guard_small,	uint64_t,	uint64_t)	\
    O(san_extents_until_guard_large,	uint64_t,	uint64_t)	\
    O(iarena,			arena_t *,		arena_t *)	\
    O(arena,			arena_t *,		arena_t *)	\
    O(arena_decay_ticker,	ticker_geom_t,		ticker_geom_t)	\
    O(sec_shard,		uint8_t,		uint8_t)	\
    O(binshards,		tsd_binshards_t,	tsd_binshards_t)\
    O(tsd_link,			tsd_link_t,		tsd_link_t)	\
    O(in_hook,			bool,			bool)		\
    O(peak,			peak_t,			peak_t)		\
    O(activity_callback_thunk,	activity_callback_thunk_t,		\
	activity_callback_thunk_t)					\
    O(tcache_slow,		tcache_slow_t,		tcache_slow_t)	\
    O(rtree_ctx,		rtree_ctx_t,		rtree_ctx_t)

#define TSD_DATA_SLOW_INITIALIZER					\
    /* tcache_enabled */	TCACHE_ENABLED_ZERO_INITIALIZER,	\
    /* reentrancy_level */	0,					\
    /* min_init_state_nfetched */	0,				\
    /* thread_allocated_last_event */	0,				\
    /* thread_allocated_next_event */	0,				\
    /* thread_deallocated_last_event */	0,				\
    /* thread_deallocated_next_event */	0,				\
    /* tcache_gc_event_wait */		0,				\
    /* tcache_gc_dalloc_event_wait */	0,				\
    /* prof_sample_event_wait */	0,				\
    /* prof_sample_last_event */	0,				\
    /* stats_interval_event_wait */	0,				\
    /* stats_interval_last_event */	0,				\
    /* peak_alloc_event_wait */		0,				\
    /* peak_dalloc_event_wait */	0,				\
    /* prof_tdata */		NULL,					\
    /* prng_state */		0,					\
    /* san_extents_until_guard_small */	0,				\
    /* san_extents_until_guard_large */	0,				\
    /* iarena */		NULL,					\
    /* arena */			NULL,					\
    /* arena_decay_ticker */						\
	TICKER_GEOM_INIT(ARENA_DECAY_NTICKS_PER_UPDATE),		\
    /* sec_shard */		(uint8_t)-1,				\
    /* binshards */		TSD_BINSHARDS_ZERO_INITIALIZER,		\
    /* tsd_link */		{NULL},					\
    /* in_hook */		false,					\
    /* peak */			PEAK_INITIALIZER,			\
    /* activity_callback_thunk */					\
	ACTIVITY_CALLBACK_THUNK_INITIALIZER,				\
    /* tcache_slow */		TCACHE_SLOW_ZERO_INITIALIZER,		\
    /* rtree_ctx */		RTREE_CTX_INITIALIZER,

/*  O(name,			type,			nullable type) */
#define TSD_DATA_FAST							\
    O(thread_allocated,		uint64_t,		uint64_t)	\
    O(thread_allocated_next_event_fast,	uint64_t,	uint64_t)	\
    O(thread_deallocated,	uint64_t,		uint64_t)	\
    O(thread_deallocated_next_event_fast, uint64_t,	uint64_t)	\
    O(tcache,			tcache_t,		tcache_t)

#define TSD_DATA_FAST_INITIALIZER					\
    /* thread_allocated */	0,					\
    /* thread_allocated_next_event_fast */ 0, 				\
    /* thread_deallocated */	0,					\
    /* thread_deallocated_next_event_fast */	0,			\
    /* tcache */		TCACHE_ZERO_INITIALIZER,

/*  O(name,			type,			nullable type) */
#define TSD_DATA_SLOWER							\
    O(witness_tsd,              witness_tsd_t,		witness_tsdn_t)	\
    MALLOC_TEST_TSD

#define TSD_DATA_SLOWER_INITIALIZER					\
    /* witness */		WITNESS_TSD_INITIALIZER			\
    /* test data */		MALLOC_TEST_TSD_INITIALIZER


#define TSD_INITIALIZER {						\
    				TSD_DATA_SLOW_INITIALIZER		\
    /* state */			ATOMIC_INIT(tsd_state_uninitialized),	\
    				TSD_DATA_FAST_INITIALIZER		\
    				TSD_DATA_SLOWER_INITIALIZER		\
}

#if defined(JEMALLOC_MALLOC_THREAD_CLEANUP) || defined(_WIN32)
void _malloc_tsd_cleanup_register(bool (*f)(void));
#endif

void *malloc_tsd_malloc(size_t size);
void malloc_tsd_dalloc(void *wrapper);
tsd_t *malloc_tsd_boot0(void);
void malloc_tsd_boot1(void);
void tsd_cleanup(void *arg);
tsd_t *tsd_fetch_slow(tsd_t *tsd, bool minimal);
void tsd_state_set(tsd_t *tsd, uint8_t new_state);
void tsd_slow_update(tsd_t *tsd);
void tsd_prefork(tsd_t *tsd);
void tsd_postfork_parent(tsd_t *tsd);
void tsd_postfork_child(tsd_t *tsd);

/*
 * Call ..._inc when your module wants to take all threads down the slow paths,
 * and ..._dec when it no longer needs to.
 */
void tsd_global_slow_inc(tsdn_t *tsdn);
void tsd_global_slow_dec(tsdn_t *tsdn);
bool tsd_global_slow(void);

#define TSD_MIN_INIT_STATE_MAX_FETCHED (128)

enum {
	/* Common case --> jnz. */
	tsd_state_nominal = 0,
	/* Initialized but on slow path. */
	tsd_state_nominal_slow = 1,
	/*
	 * Some thread has changed global state in such a way that all nominal
	 * threads need to recompute their fast / slow status the next time they
	 * get a chance.
	 *
	 * Any thread can change another thread's status *to* recompute, but
	 * threads are the only ones who can change their status *from*
	 * recompute.
	 */
	tsd_state_nominal_recompute = 2,
	/*
	 * The above nominal states should be lower values.  We use
	 * tsd_nominal_max to separate nominal states from threads in the
	 * process of being born / dying.
	 */
	tsd_state_nominal_max = 2,

	/*
	 * A thread might free() during its death as its only allocator action;
	 * in such scenarios, we need tsd, but set up in such a way that no
	 * cleanup is necessary.
	 */
	tsd_state_minimal_initialized = 3,
	/* States during which we know we're in thread death. */
	tsd_state_purgatory = 4,
	tsd_state_reincarnated = 5,
	/*
	 * What it says on the tin; tsd that hasn't been initialized.  Note
	 * that even when the tsd struct lives in TLS, when need to keep track
	 * of stuff like whether or not our pthread destructors have been
	 * scheduled, so this really truly is different than the nominal state.
	 */
	tsd_state_uninitialized = 6
};

/*
 * Some TSD accesses can only be done in a nominal state.  To enforce this, we
 * wrap TSD member access in a function that asserts on TSD state, and mangle
 * field names to prevent touching them accidentally.
 */
#define TSD_MANGLE(n) cant_access_tsd_items_directly_use_a_getter_or_setter_##n

#ifdef JEMALLOC_U8_ATOMICS
#  define tsd_state_t atomic_u8_t
#  define tsd_atomic_load atomic_load_u8
#  define tsd_atomic_store atomic_store_u8
#  define tsd_atomic_exchange atomic_exchange_u8
#else
#  define tsd_state_t atomic_u32_t
#  define tsd_atomic_load atomic_load_u32
#  define tsd_atomic_store atomic_store_u32
#  define tsd_atomic_exchange atomic_exchange_u32
#endif

/* The actual tsd. */
struct tsd_s {
	/*
	 * The contents should be treated as totally opaque outside the tsd
	 * module.  Access any thread-local state through the getters and
	 * setters below.
	 */

#define O(n, t, nt)							\
	t TSD_MANGLE(n);

	TSD_DATA_SLOW
	/*
	 * We manually limit the state to just a single byte.  Unless the 8-bit
	 * atomics are unavailable (which is rare).
	 */
	tsd_state_t state;
	TSD_DATA_FAST
	TSD_DATA_SLOWER
#undef O
};

JEMALLOC_ALWAYS_INLINE uint8_t
tsd_state_get(tsd_t *tsd) {
	/*
	 * This should be atomic.  Unfortunately, compilers right now can't tell
	 * that this can be done as a memory comparison, and forces a load into
	 * a register that hurts fast-path performance.
	 */
	/* return atomic_load_u8(&tsd->state, ATOMIC_RELAXED); */
	return *(uint8_t *)&tsd->state;
}

/*
 * Wrapper around tsd_t that makes it possible to avoid implicit conversion
 * between tsd_t and tsdn_t, where tsdn_t is "nullable" and has to be
 * explicitly converted to tsd_t, which is non-nullable.
 */
struct tsdn_s {
	tsd_t tsd;
};
#define TSDN_NULL ((tsdn_t *)0)
JEMALLOC_ALWAYS_INLINE tsdn_t *
tsd_tsdn(tsd_t *tsd) {
	return (tsdn_t *)tsd;
}

JEMALLOC_ALWAYS_INLINE bool
tsdn_null(const tsdn_t *tsdn) {
	return tsdn == NULL;
}

JEMALLOC_ALWAYS_INLINE tsd_t *
tsdn_tsd(tsdn_t *tsdn) {
	assert(!tsdn_null(tsdn));

	return &tsdn->tsd;
}
