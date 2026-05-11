#ifndef JEMALLOC_INTERNAL_BIN_H
#define JEMALLOC_INTERNAL_BIN_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/batcher.h"
#include "jemalloc/internal/bin_stats.h"
#include "jemalloc/internal/bin_types.h"
#include "jemalloc/internal/edata.h"
#include "jemalloc/internal/mutex.h"
#include "jemalloc/internal/sc.h"

#define BIN_REMOTE_FREE_ELEMS_MAX 16

#ifdef JEMALLOC_JET
extern void (*bin_batching_test_after_push_hook)(size_t idx);
extern void (*bin_batching_test_mid_pop_hook)(size_t elems_to_pop);
extern void (*bin_batching_test_after_unlock_hook)(unsigned slab_dalloc_count,
    bool list_empty);
#endif

#ifdef JEMALLOC_JET
extern unsigned bin_batching_test_ndalloc_slabs_max;
#else
static const unsigned bin_batching_test_ndalloc_slabs_max = (unsigned)-1;
#endif

JEMALLOC_ALWAYS_INLINE void
bin_batching_test_after_push(size_t idx) {
	(void)idx;
#ifdef JEMALLOC_JET
	if (bin_batching_test_after_push_hook != NULL) {
		bin_batching_test_after_push_hook(idx);
	}
#endif
}

JEMALLOC_ALWAYS_INLINE void
bin_batching_test_mid_pop(size_t elems_to_pop) {
	(void)elems_to_pop;
#ifdef JEMALLOC_JET
	if (bin_batching_test_mid_pop_hook != NULL) {
		bin_batching_test_mid_pop_hook(elems_to_pop);
	}
#endif
}

JEMALLOC_ALWAYS_INLINE void
bin_batching_test_after_unlock(unsigned slab_dalloc_count, bool list_empty) {
	(void)slab_dalloc_count;
	(void)list_empty;
#ifdef JEMALLOC_JET
	if (bin_batching_test_after_unlock_hook != NULL) {
		bin_batching_test_after_unlock_hook(slab_dalloc_count,
		    list_empty);
	}
#endif
}

/*
 * A bin contains a set of extents that are currently being used for slab
 * allocations.
 */
typedef struct bin_s bin_t;
struct bin_s {
	/* All operations on bin_t fields require lock ownership. */
	malloc_mutex_t		lock;

	/*
	 * Bin statistics.  These get touched every time the lock is acquired,
	 * so put them close by in the hopes of getting some cache locality.
	 */
	bin_stats_t	stats;

	/*
	 * Current slab being used to service allocations of this bin's size
	 * class.  slabcur is independent of slabs_{nonfull,full}; whenever
	 * slabcur is reassigned, the previous slab must be deallocated or
	 * inserted into slabs_{nonfull,full}.
	 */
	edata_t			*slabcur;

	/*
	 * Heap of non-full slabs.  This heap is used to assure that new
	 * allocations come from the non-full slab that is oldest/lowest in
	 * memory.
	 */
	edata_heap_t		slabs_nonfull;

	/* List used to track full slabs. */
	edata_list_active_t	slabs_full;
};

typedef struct bin_remote_free_data_s bin_remote_free_data_t;
struct bin_remote_free_data_s {
	void *ptr;
	edata_t *slab;
};

typedef struct bin_with_batch_s bin_with_batch_t;
struct bin_with_batch_s {
	bin_t bin;
	batcher_t remote_frees;
	bin_remote_free_data_t remote_free_data[BIN_REMOTE_FREE_ELEMS_MAX];
};

/* A set of sharded bins of the same size class. */
typedef struct bins_s bins_t;
struct bins_s {
	/* Sharded bins.  Dynamically sized. */
	bin_t *bin_shards;
};

void bin_shard_sizes_boot(unsigned bin_shard_sizes[SC_NBINS]);
bool bin_update_shard_size(unsigned bin_shards[SC_NBINS], size_t start_size,
    size_t end_size, size_t nshards);

/* Initializes a bin to empty.  Returns true on error. */
bool bin_init(bin_t *bin, unsigned binind);

/* Forking. */
void bin_prefork(tsdn_t *tsdn, bin_t *bin, bool has_batch);
void bin_postfork_parent(tsdn_t *tsdn, bin_t *bin, bool has_batch);
void bin_postfork_child(tsdn_t *tsdn, bin_t *bin, bool has_batch);

/* Stats. */
static inline void
bin_stats_merge(tsdn_t *tsdn, bin_stats_data_t *dst_bin_stats, bin_t *bin) {
	malloc_mutex_lock(tsdn, &bin->lock);
	malloc_mutex_prof_accum(tsdn, &dst_bin_stats->mutex_data, &bin->lock);
	bin_stats_t *stats = &dst_bin_stats->stats_data;
	stats->nmalloc += bin->stats.nmalloc;
	stats->ndalloc += bin->stats.ndalloc;
	stats->nrequests += bin->stats.nrequests;
	stats->curregs += bin->stats.curregs;
	stats->nfills += bin->stats.nfills;
	stats->nflushes += bin->stats.nflushes;
	stats->nslabs += bin->stats.nslabs;
	stats->reslabs += bin->stats.reslabs;
	stats->curslabs += bin->stats.curslabs;
	stats->nonfull_slabs += bin->stats.nonfull_slabs;

	stats->batch_failed_pushes += bin->stats.batch_failed_pushes;
	stats->batch_pushes += bin->stats.batch_pushes;
	stats->batch_pushed_elems += bin->stats.batch_pushed_elems;

	malloc_mutex_unlock(tsdn, &bin->lock);
}

#endif /* JEMALLOC_INTERNAL_BIN_H */
