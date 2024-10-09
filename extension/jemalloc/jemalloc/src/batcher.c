#include "jemalloc/internal/jemalloc_preamble.h"

#include "jemalloc/internal/batcher.h"

#include "jemalloc/internal/assert.h"
#include "jemalloc/internal/atomic.h"

void
batcher_init(batcher_t *batcher, size_t nelems_max) {
	atomic_store_zu(&batcher->nelems, 0, ATOMIC_RELAXED);
	batcher->nelems_max = nelems_max;
	batcher->npushes = 0;
	malloc_mutex_init(&batcher->mtx, "batcher", WITNESS_RANK_BATCHER,
	    malloc_mutex_rank_exclusive);
}

/*
 * Returns an index (into some user-owned array) to use for pushing, or
 * BATCHER_NO_IDX if no index is free.
 */
size_t batcher_push_begin(tsdn_t *tsdn, batcher_t *batcher,
    size_t elems_to_push) {
	assert(elems_to_push > 0);
	size_t nelems_guess = atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED);
	if (nelems_guess + elems_to_push > batcher->nelems_max) {
		return BATCHER_NO_IDX;
	}
	malloc_mutex_lock(tsdn, &batcher->mtx);
	size_t nelems = atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED);
	if (nelems + elems_to_push > batcher->nelems_max) {
		malloc_mutex_unlock(tsdn, &batcher->mtx);
		return BATCHER_NO_IDX;
	}
	assert(elems_to_push <= batcher->nelems_max - nelems);
	/*
	 * We update nelems at push time (instead of during pop) so that other
	 * racing accesses of the batcher can fail fast instead of trying to
	 * acquire a mutex only to discover that there's no space for them.
	 */
	atomic_store_zu(&batcher->nelems, nelems + elems_to_push, ATOMIC_RELAXED);
	batcher->npushes++;
	return nelems;
}

size_t
batcher_pop_get_pushes(tsdn_t *tsdn, batcher_t *batcher) {
	malloc_mutex_assert_owner(tsdn, &batcher->mtx);
	size_t npushes = batcher->npushes;
	batcher->npushes = 0;
	return npushes;
}

void
batcher_push_end(tsdn_t *tsdn, batcher_t *batcher) {
	malloc_mutex_assert_owner(tsdn, &batcher->mtx);
	assert(atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED) > 0);
	malloc_mutex_unlock(tsdn, &batcher->mtx);
}

size_t
batcher_pop_begin(tsdn_t *tsdn, batcher_t *batcher) {
	size_t nelems_guess = atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED);
	assert(nelems_guess <= batcher->nelems_max);
	if (nelems_guess == 0) {
		return BATCHER_NO_IDX;
	}
	malloc_mutex_lock(tsdn, &batcher->mtx);
	size_t nelems = atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED);
	assert(nelems <= batcher->nelems_max);
	if (nelems == 0) {
		malloc_mutex_unlock(tsdn, &batcher->mtx);
		return BATCHER_NO_IDX;
	}
	atomic_store_zu(&batcher->nelems, 0, ATOMIC_RELAXED);
	return nelems;
}

void batcher_pop_end(tsdn_t *tsdn, batcher_t *batcher) {
	assert(atomic_load_zu(&batcher->nelems, ATOMIC_RELAXED) == 0);
	malloc_mutex_unlock(tsdn, &batcher->mtx);
}

void
batcher_prefork(tsdn_t *tsdn, batcher_t *batcher) {
	malloc_mutex_prefork(tsdn, &batcher->mtx);
}

void
batcher_postfork_parent(tsdn_t *tsdn, batcher_t *batcher) {
	malloc_mutex_postfork_parent(tsdn, &batcher->mtx);
}

void
batcher_postfork_child(tsdn_t *tsdn, batcher_t *batcher) {
	malloc_mutex_postfork_child(tsdn, &batcher->mtx);
}
