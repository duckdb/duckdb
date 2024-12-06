#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/assert.h"
#include "jemalloc/internal/bin.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/witness.h"

#ifdef JEMALLOC_JET
unsigned bin_batching_test_ndalloc_slabs_max = (unsigned)-1;
void (*bin_batching_test_after_push_hook)(size_t push_idx);
void (*bin_batching_test_mid_pop_hook)(size_t nelems_to_pop);
void (*bin_batching_test_after_unlock_hook)(unsigned slab_dalloc_count,
    bool list_empty);
#endif

bool
bin_update_shard_size(unsigned bin_shard_sizes[SC_NBINS], size_t start_size,
    size_t end_size, size_t nshards) {
	if (nshards > BIN_SHARDS_MAX || nshards == 0) {
		return true;
	}

	if (start_size > SC_SMALL_MAXCLASS) {
		return false;
	}
	if (end_size > SC_SMALL_MAXCLASS) {
		end_size = SC_SMALL_MAXCLASS;
	}

	/* Compute the index since this may happen before sz init. */
	szind_t ind1 = sz_size2index_compute(start_size);
	szind_t ind2 = sz_size2index_compute(end_size);
	for (unsigned i = ind1; i <= ind2; i++) {
		bin_shard_sizes[i] = (unsigned)nshards;
	}

	return false;
}

void
bin_shard_sizes_boot(unsigned bin_shard_sizes[SC_NBINS]) {
	/* Load the default number of shards. */
	for (unsigned i = 0; i < SC_NBINS; i++) {
		bin_shard_sizes[i] = N_BIN_SHARDS_DEFAULT;
	}
}

bool
bin_init(bin_t *bin, unsigned binind) {
	if (malloc_mutex_init(&bin->lock, "bin", WITNESS_RANK_BIN,
	    malloc_mutex_rank_exclusive)) {
		return true;
	}
	bin->slabcur = NULL;
	edata_heap_new(&bin->slabs_nonfull);
	edata_list_active_init(&bin->slabs_full);
	if (config_stats) {
		memset(&bin->stats, 0, sizeof(bin_stats_t));
	}
	if (arena_bin_has_batch(binind)) {
		bin_with_batch_t *batched_bin = (bin_with_batch_t *)bin;
		batcher_init(&batched_bin->remote_frees,
		    opt_bin_info_remote_free_max);
	}
	return false;
}

void
bin_prefork(tsdn_t *tsdn, bin_t *bin, bool has_batch) {
	malloc_mutex_prefork(tsdn, &bin->lock);
	if (has_batch) {
		/*
		 * The batch mutex has lower rank than the bin mutex (as it must
		 * -- it's acquired later).  But during forking, we go
		 *  bin-at-a-time, so that we acquire mutex on bin 0, then on
		 *  the bin 0 batcher, then on bin 1.  This is a safe ordering
		 *  (it's ordered by the index of arenas and bins within those
		 *  arenas), but will trigger witness errors that would
		 *  otherwise force another level of arena forking that breaks
		 *  bin encapsulation (because the witness API doesn't "know"
		 *  about arena or bin ordering -- it just sees that the batcher
		 *  has a lower rank than the bin).  So instead we exclude the
		 *  batcher mutex from witness checking during fork (which is
		 *  the only time we touch multiple bins at once) by passing
		 *  TSDN_NULL.
		 */
		bin_with_batch_t *batched = (bin_with_batch_t *)bin;
		batcher_prefork(TSDN_NULL, &batched->remote_frees);
	}
}

void
bin_postfork_parent(tsdn_t *tsdn, bin_t *bin, bool has_batch) {
	malloc_mutex_postfork_parent(tsdn, &bin->lock);
	if (has_batch) {
		bin_with_batch_t *batched = (bin_with_batch_t *)bin;
		batcher_postfork_parent(TSDN_NULL, &batched->remote_frees);
	}
}

void
bin_postfork_child(tsdn_t *tsdn, bin_t *bin, bool has_batch) {
	malloc_mutex_postfork_child(tsdn, &bin->lock);
	if (has_batch) {
		bin_with_batch_t *batched = (bin_with_batch_t *)bin;
		batcher_postfork_child(TSDN_NULL, &batched->remote_frees);
	}
}
