#ifndef JEMALLOC_INTERNAL_BATCHER_H
#define JEMALLOC_INTERNAL_BATCHER_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/atomic.h"
#include "jemalloc/internal/mutex.h"

#define BATCHER_NO_IDX ((size_t)-1)

typedef struct batcher_s batcher_t;
struct batcher_s {
	/*
	 * Optimize for locality -- nelems_max and nelems are always touched
	 * togehter, along with the front of the mutex. The end of the mutex is
	 * only touched if there's contention.
	 */
	atomic_zu_t nelems;
	size_t nelems_max;
	size_t npushes;
	malloc_mutex_t mtx;
};

void batcher_init(batcher_t *batcher, size_t nelems_max);

/*
 * Returns an index (into some user-owned array) to use for pushing, or
 * BATCHER_NO_IDX if no index is free.  If the former, the caller must call
 * batcher_push_end once done.
 */
size_t batcher_push_begin(tsdn_t *tsdn, batcher_t *batcher,
    size_t elems_to_push);
void batcher_push_end(tsdn_t *tsdn, batcher_t *batcher);

/*
 * Returns the number of items to pop, or BATCHER_NO_IDX if there are none.
 * If the former, must be followed by a call to batcher_pop_end.
 */
size_t batcher_pop_begin(tsdn_t *tsdn, batcher_t *batcher);
size_t batcher_pop_get_pushes(tsdn_t *tsdn, batcher_t *batcher);
void batcher_pop_end(tsdn_t *tsdn, batcher_t *batcher);

void batcher_prefork(tsdn_t *tsdn, batcher_t *batcher);
void batcher_postfork_parent(tsdn_t *tsdn, batcher_t *batcher);
void batcher_postfork_child(tsdn_t *tsdn, batcher_t *batcher);

#endif /* JEMALLOC_INTERNAL_BATCHER_H */
