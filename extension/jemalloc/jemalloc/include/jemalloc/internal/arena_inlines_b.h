#ifndef JEMALLOC_INTERNAL_ARENA_INLINES_B_H
#define JEMALLOC_INTERNAL_ARENA_INLINES_B_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/arena_externs.h"
#include "jemalloc/internal/arena_structs.h"
#include "jemalloc/internal/div.h"
#include "jemalloc/internal/emap.h"
#include "jemalloc/internal/jemalloc_internal_inlines_b.h"
#include "jemalloc/internal/jemalloc_internal_types.h"
#include "jemalloc/internal/large_externs.h"
#include "jemalloc/internal/mutex.h"
#include "jemalloc/internal/prof_externs.h"
#include "jemalloc/internal/prof_structs.h"
#include "jemalloc/internal/rtree.h"
#include "jemalloc/internal/safety_check.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"
#include "jemalloc/internal/tcache_inlines.h"
#include "jemalloc/internal/ticker.h"

static inline arena_t *
arena_get_from_edata(edata_t *edata) {
	return (arena_t *)atomic_load_p(&arenas[edata_arena_ind_get(edata)],
	    ATOMIC_RELAXED);
}

JEMALLOC_ALWAYS_INLINE arena_t *
arena_choose_maybe_huge(tsd_t *tsd, arena_t *arena, size_t size) {
	if (arena != NULL) {
		return arena;
	}

	/*
	 * For huge allocations, use the dedicated huge arena if both are true:
	 * 1) is using auto arena selection (i.e. arena == NULL), and 2) the
	 * thread is not assigned to a manual arena.
	 */
	arena_t *tsd_arena = tsd_arena_get(tsd);
	if (tsd_arena == NULL) {
		tsd_arena = arena_choose(tsd, NULL);
	}

	size_t threshold = atomic_load_zu(
	    &tsd_arena->pa_shard.pac.oversize_threshold, ATOMIC_RELAXED);
	if (unlikely(size >= threshold) && arena_is_auto(tsd_arena)) {
		return arena_choose_huge(tsd);
	}

	return tsd_arena;
}

JEMALLOC_ALWAYS_INLINE bool
large_dalloc_safety_checks(edata_t *edata, const void *ptr, szind_t szind) {
	if (!config_opt_safety_checks) {
		return false;
	}

	/*
	 * Eagerly detect double free and sized dealloc bugs for large sizes.
	 * The cost is low enough (as edata will be accessed anyway) to be
	 * enabled all the time.
	 */
	if (unlikely(edata == NULL ||
	    edata_state_get(edata) != extent_state_active)) {
		safety_check_fail("Invalid deallocation detected: "
		    "pages being freed (%p) not currently active, "
		    "possibly caused by double free bugs.", ptr);
		return true;
	}
	size_t input_size = sz_index2size(szind);
	if (unlikely(input_size != edata_usize_get(edata))) {
		safety_check_fail_sized_dealloc(/* current_dealloc */ true, ptr,
		    /* true_size */ edata_usize_get(edata), input_size);
		return true;
	}

	return false;
}

JEMALLOC_ALWAYS_INLINE void
arena_prof_info_get(tsd_t *tsd, const void *ptr, emap_alloc_ctx_t *alloc_ctx,
    prof_info_t *prof_info, bool reset_recent) {
	cassert(config_prof);
	assert(ptr != NULL);
	assert(prof_info != NULL);

	edata_t *edata = NULL;
	bool is_slab;

	/* Static check. */
	if (alloc_ctx == NULL) {
		edata = emap_edata_lookup(tsd_tsdn(tsd), &arena_emap_global,
		    ptr);
		is_slab = edata_slab_get(edata);
	} else if (unlikely(!(is_slab = alloc_ctx->slab))) {
		edata = emap_edata_lookup(tsd_tsdn(tsd), &arena_emap_global,
		    ptr);
	}

	if (unlikely(!is_slab)) {
		/* edata must have been initialized at this point. */
		assert(edata != NULL);
		if (reset_recent &&
		    large_dalloc_safety_checks(edata, ptr,
		    edata_szind_get(edata))) {
			prof_info->alloc_tctx = PROF_TCTX_SENTINEL;
			return;
		}
		large_prof_info_get(tsd, edata, prof_info, reset_recent);
	} else {
		prof_info->alloc_tctx = PROF_TCTX_SENTINEL;
		/*
		 * No need to set other fields in prof_info; they will never be
		 * accessed if alloc_tctx == PROF_TCTX_SENTINEL.
		 */
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_prof_tctx_reset(tsd_t *tsd, const void *ptr,
    emap_alloc_ctx_t *alloc_ctx) {
	cassert(config_prof);
	assert(ptr != NULL);

	/* Static check. */
	if (alloc_ctx == NULL) {
		edata_t *edata = emap_edata_lookup(tsd_tsdn(tsd),
		    &arena_emap_global, ptr);
		if (unlikely(!edata_slab_get(edata))) {
			large_prof_tctx_reset(edata);
		}
	} else {
		if (unlikely(!alloc_ctx->slab)) {
			edata_t *edata = emap_edata_lookup(tsd_tsdn(tsd),
			    &arena_emap_global, ptr);
			large_prof_tctx_reset(edata);
		}
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_prof_tctx_reset_sampled(tsd_t *tsd, const void *ptr) {
	cassert(config_prof);
	assert(ptr != NULL);

	edata_t *edata = emap_edata_lookup(tsd_tsdn(tsd), &arena_emap_global,
	    ptr);
	assert(!edata_slab_get(edata));

	large_prof_tctx_reset(edata);
}

JEMALLOC_ALWAYS_INLINE void
arena_prof_info_set(tsd_t *tsd, edata_t *edata, prof_tctx_t *tctx,
    size_t size) {
	cassert(config_prof);

	assert(!edata_slab_get(edata));
	large_prof_info_set(edata, tctx, size);
}

JEMALLOC_ALWAYS_INLINE void
arena_decay_ticks(tsdn_t *tsdn, arena_t *arena, unsigned nticks) {
	if (unlikely(tsdn_null(tsdn))) {
		return;
	}
	tsd_t *tsd = tsdn_tsd(tsdn);
	/*
	 * We use the ticker_geom_t to avoid having per-arena state in the tsd.
	 * Instead of having a countdown-until-decay timer running for every
	 * arena in every thread, we flip a coin once per tick, whose
	 * probability of coming up heads is 1/nticks; this is effectively the
	 * operation of the ticker_geom_t.  Each arena has the same chance of a
	 * coinflip coming up heads (1/ARENA_DECAY_NTICKS_PER_UPDATE), so we can
	 * use a single ticker for all of them.
	 */
	ticker_geom_t *decay_ticker = tsd_arena_decay_tickerp_get(tsd);
	uint64_t *prng_state = tsd_prng_statep_get(tsd);
	if (unlikely(ticker_geom_ticks(decay_ticker, prng_state, nticks,
	    tsd_reentrancy_level_get(tsd) > 0))) {
		arena_decay(tsdn, arena, false, false);
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_decay_tick(tsdn_t *tsdn, arena_t *arena) {
	arena_decay_ticks(tsdn, arena, 1);
}

JEMALLOC_ALWAYS_INLINE void *
arena_malloc(tsdn_t *tsdn, arena_t *arena, size_t size, szind_t ind, bool zero,
    bool slab, tcache_t *tcache, bool slow_path) {
	assert(!tsdn_null(tsdn) || tcache == NULL);

	if (likely(tcache != NULL)) {
		if (likely(slab)) {
			assert(sz_can_use_slab(size));
			return tcache_alloc_small(tsdn_tsd(tsdn), arena,
			    tcache, size, ind, zero, slow_path);
		} else if (likely(
		    ind < tcache_nbins_get(tcache->tcache_slow) &&
		    !tcache_bin_disabled(ind, &tcache->bins[ind],
		    tcache->tcache_slow))) {
			return tcache_alloc_large(tsdn_tsd(tsdn), arena,
			    tcache, size, ind, zero, slow_path);
		}
		/* (size > tcache_max) case falls through. */
	}

	return arena_malloc_hard(tsdn, arena, size, ind, zero, slab);
}

JEMALLOC_ALWAYS_INLINE arena_t *
arena_aalloc(tsdn_t *tsdn, const void *ptr) {
	edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global, ptr);
	unsigned arena_ind = edata_arena_ind_get(edata);
	return (arena_t *)atomic_load_p(&arenas[arena_ind], ATOMIC_RELAXED);
}

JEMALLOC_ALWAYS_INLINE size_t
arena_salloc(tsdn_t *tsdn, const void *ptr) {
	assert(ptr != NULL);
	emap_alloc_ctx_t alloc_ctx;
	emap_alloc_ctx_lookup(tsdn, &arena_emap_global, ptr, &alloc_ctx);
	assert(alloc_ctx.szind != SC_NSIZES);

	return sz_index2size(alloc_ctx.szind);
}

JEMALLOC_ALWAYS_INLINE size_t
arena_vsalloc(tsdn_t *tsdn, const void *ptr) {
	/*
	 * Return 0 if ptr is not within an extent managed by jemalloc.  This
	 * function has two extra costs relative to isalloc():
	 * - The rtree calls cannot claim to be dependent lookups, which induces
	 *   rtree lookup load dependencies.
	 * - The lookup may fail, so there is an extra branch to check for
	 *   failure.
	 */

	emap_full_alloc_ctx_t full_alloc_ctx;
	bool missing = emap_full_alloc_ctx_try_lookup(tsdn, &arena_emap_global,
	    ptr, &full_alloc_ctx);
	if (missing) {
		return 0;
	}

	if (full_alloc_ctx.edata == NULL) {
		return 0;
	}
	assert(edata_state_get(full_alloc_ctx.edata) == extent_state_active);
	/* Only slab members should be looked up via interior pointers. */
	assert(edata_addr_get(full_alloc_ctx.edata) == ptr
	    || edata_slab_get(full_alloc_ctx.edata));

	assert(full_alloc_ctx.szind != SC_NSIZES);

	return sz_index2size(full_alloc_ctx.szind);
}

static inline void
arena_dalloc_large_no_tcache(tsdn_t *tsdn, void *ptr, szind_t szind) {
	if (config_prof && unlikely(szind < SC_NBINS)) {
		arena_dalloc_promoted(tsdn, ptr, NULL, true);
	} else {
		edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global,
		    ptr);
		if (large_dalloc_safety_checks(edata, ptr, szind)) {
			/* See the comment in isfree. */
			return;
		}
		large_dalloc(tsdn, edata);
	}
}

static inline void
arena_dalloc_no_tcache(tsdn_t *tsdn, void *ptr) {
	assert(ptr != NULL);

	emap_alloc_ctx_t alloc_ctx;
	emap_alloc_ctx_lookup(tsdn, &arena_emap_global, ptr, &alloc_ctx);

	if (config_debug) {
		edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global,
		    ptr);
		assert(alloc_ctx.szind == edata_szind_get(edata));
		assert(alloc_ctx.szind < SC_NSIZES);
		assert(alloc_ctx.slab == edata_slab_get(edata));
	}

	if (likely(alloc_ctx.slab)) {
		/* Small allocation. */
		arena_dalloc_small(tsdn, ptr);
	} else {
		arena_dalloc_large_no_tcache(tsdn, ptr, alloc_ctx.szind);
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_dalloc_large(tsdn_t *tsdn, void *ptr, tcache_t *tcache, szind_t szind,
    bool slow_path) {
	assert (!tsdn_null(tsdn) && tcache != NULL);
	bool is_sample_promoted = config_prof && szind < SC_NBINS;
	if (unlikely(is_sample_promoted)) {
		arena_dalloc_promoted(tsdn, ptr, tcache, slow_path);
	} else {
		if (szind < tcache_nbins_get(tcache->tcache_slow) &&
		    !tcache_bin_disabled(szind, &tcache->bins[szind],
		    tcache->tcache_slow)) {
			tcache_dalloc_large(tsdn_tsd(tsdn), tcache, ptr, szind,
			    slow_path);
		} else {
			edata_t *edata = emap_edata_lookup(tsdn,
			    &arena_emap_global, ptr);
			if (large_dalloc_safety_checks(edata, ptr, szind)) {
				/* See the comment in isfree. */
				return;
			}
			large_dalloc(tsdn, edata);
		}
	}
}

/* Find the region index of a pointer. */
JEMALLOC_ALWAYS_INLINE size_t
arena_slab_regind_impl(div_info_t* div_info, szind_t binind,
    edata_t *slab, const void *ptr) {
	size_t diff, regind;

	/* Freeing a pointer outside the slab can cause assertion failure. */
	assert((uintptr_t)ptr >= (uintptr_t)edata_addr_get(slab));
	assert((uintptr_t)ptr < (uintptr_t)edata_past_get(slab));
	/* Freeing an interior pointer can cause assertion failure. */
	assert(((uintptr_t)ptr - (uintptr_t)edata_addr_get(slab)) %
	    (uintptr_t)bin_infos[binind].reg_size == 0);

	diff = (size_t)((uintptr_t)ptr - (uintptr_t)edata_addr_get(slab));

	/* Avoid doing division with a variable divisor. */
	regind = div_compute(div_info, diff);
	assert(regind < bin_infos[binind].nregs);
	return regind;
}

/* Checks whether ptr is currently active in the arena. */
JEMALLOC_ALWAYS_INLINE bool
arena_tcache_dalloc_small_safety_check(tsdn_t *tsdn, void *ptr) {
	if (!config_debug) {
		return false;
	}
	edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global, ptr);
	szind_t binind = edata_szind_get(edata);
	div_info_t div_info = arena_binind_div_info[binind];
	/*
	 * Calls the internal function arena_slab_regind_impl because the
	 * safety check does not require a lock.
	 */
	size_t regind = arena_slab_regind_impl(&div_info, binind, edata, ptr);
	slab_data_t *slab_data = edata_slab_data_get(edata);
	const bin_info_t *bin_info = &bin_infos[binind];
	assert(edata_nfree_get(edata) < bin_info->nregs);
	if (unlikely(!bitmap_get(slab_data->bitmap, &bin_info->bitmap_info,
	    regind))) {
		safety_check_fail(
		    "Invalid deallocation detected: the pointer being freed (%p) not "
		    "currently active, possibly caused by double free bugs.\n", ptr);
		return true;
	}
	return false;
}

JEMALLOC_ALWAYS_INLINE void
arena_dalloc(tsdn_t *tsdn, void *ptr, tcache_t *tcache,
    emap_alloc_ctx_t *caller_alloc_ctx, bool slow_path) {
	assert(!tsdn_null(tsdn) || tcache == NULL);
	assert(ptr != NULL);

	if (unlikely(tcache == NULL)) {
		arena_dalloc_no_tcache(tsdn, ptr);
		return;
	}

	emap_alloc_ctx_t alloc_ctx;
	if (caller_alloc_ctx != NULL) {
		alloc_ctx = *caller_alloc_ctx;
	} else {
		util_assume(tsdn != NULL);
		emap_alloc_ctx_lookup(tsdn, &arena_emap_global, ptr,
		    &alloc_ctx);
	}

	if (config_debug) {
		edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global,
		    ptr);
		assert(alloc_ctx.szind == edata_szind_get(edata));
		assert(alloc_ctx.szind < SC_NSIZES);
		assert(alloc_ctx.slab == edata_slab_get(edata));
	}

	if (likely(alloc_ctx.slab)) {
		/* Small allocation. */
		if (arena_tcache_dalloc_small_safety_check(tsdn, ptr)) {
			return;
		}
		tcache_dalloc_small(tsdn_tsd(tsdn), tcache, ptr,
		    alloc_ctx.szind, slow_path);
	} else {
		arena_dalloc_large(tsdn, ptr, tcache, alloc_ctx.szind,
		    slow_path);
	}
}

static inline void
arena_sdalloc_no_tcache(tsdn_t *tsdn, void *ptr, size_t size) {
	assert(ptr != NULL);
	assert(size <= SC_LARGE_MAXCLASS);

	emap_alloc_ctx_t alloc_ctx;
	if (!config_prof || !opt_prof) {
		/*
		 * There is no risk of being confused by a promoted sampled
		 * object, so base szind and slab on the given size.
		 */
		alloc_ctx.szind = sz_size2index(size);
		alloc_ctx.slab = (alloc_ctx.szind < SC_NBINS);
	}

	if ((config_prof && opt_prof) || config_debug) {
		emap_alloc_ctx_lookup(tsdn, &arena_emap_global, ptr,
		    &alloc_ctx);

		assert(alloc_ctx.szind == sz_size2index(size));
		assert((config_prof && opt_prof)
		    || alloc_ctx.slab == (alloc_ctx.szind < SC_NBINS));

		if (config_debug) {
			edata_t *edata = emap_edata_lookup(tsdn,
			    &arena_emap_global, ptr);
			assert(alloc_ctx.szind == edata_szind_get(edata));
			assert(alloc_ctx.slab == edata_slab_get(edata));
		}
	}

	if (likely(alloc_ctx.slab)) {
		/* Small allocation. */
		arena_dalloc_small(tsdn, ptr);
	} else {
		arena_dalloc_large_no_tcache(tsdn, ptr, alloc_ctx.szind);
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_sdalloc(tsdn_t *tsdn, void *ptr, size_t size, tcache_t *tcache,
    emap_alloc_ctx_t *caller_alloc_ctx, bool slow_path) {
	assert(!tsdn_null(tsdn) || tcache == NULL);
	assert(ptr != NULL);
	assert(size <= SC_LARGE_MAXCLASS);

	if (unlikely(tcache == NULL)) {
		arena_sdalloc_no_tcache(tsdn, ptr, size);
		return;
	}

	emap_alloc_ctx_t alloc_ctx;
	if (config_prof && opt_prof) {
		if (caller_alloc_ctx == NULL) {
			/* Uncommon case and should be a static check. */
			emap_alloc_ctx_lookup(tsdn, &arena_emap_global, ptr,
			    &alloc_ctx);
			assert(alloc_ctx.szind == sz_size2index(size));
		} else {
			alloc_ctx = *caller_alloc_ctx;
		}
	} else {
		/*
		 * There is no risk of being confused by a promoted sampled
		 * object, so base szind and slab on the given size.
		 */
		alloc_ctx.szind = sz_size2index(size);
		alloc_ctx.slab = (alloc_ctx.szind < SC_NBINS);
	}

	if (config_debug) {
		edata_t *edata = emap_edata_lookup(tsdn, &arena_emap_global,
		    ptr);
		assert(alloc_ctx.szind == edata_szind_get(edata));
		assert(alloc_ctx.slab == edata_slab_get(edata));
	}

	if (likely(alloc_ctx.slab)) {
		/* Small allocation. */
		if (arena_tcache_dalloc_small_safety_check(tsdn, ptr)) {
			return;
		}
		tcache_dalloc_small(tsdn_tsd(tsdn), tcache, ptr,
		    alloc_ctx.szind, slow_path);
	} else {
		arena_dalloc_large(tsdn, ptr, tcache, alloc_ctx.szind,
		    slow_path);
	}
}

static inline void
arena_cache_oblivious_randomize(tsdn_t *tsdn, arena_t *arena, edata_t *edata,
    size_t alignment) {
	assert(edata_base_get(edata) == edata_addr_get(edata));

	if (alignment < PAGE) {
		unsigned lg_range = LG_PAGE -
		    lg_floor(CACHELINE_CEILING(alignment));
		size_t r;
		if (!tsdn_null(tsdn)) {
			tsd_t *tsd = tsdn_tsd(tsdn);
			r = (size_t)prng_lg_range_u64(
			    tsd_prng_statep_get(tsd), lg_range);
		} else {
			uint64_t stack_value = (uint64_t)(uintptr_t)&r;
			r = (size_t)prng_lg_range_u64(&stack_value, lg_range);
		}
		uintptr_t random_offset = ((uintptr_t)r) << (LG_PAGE -
		    lg_range);
		edata->e_addr = (void *)((byte_t *)edata->e_addr +
		    random_offset);
		assert(ALIGNMENT_ADDR2BASE(edata->e_addr, alignment) ==
		    edata->e_addr);
	}
}

/*
 * The dalloc bin info contains just the information that the common paths need
 * during tcache flushes.  By force-inlining these paths, and using local copies
 * of data (so that the compiler knows it's constant), we avoid a whole bunch of
 * redundant loads and stores by leaving this information in registers.
 */
typedef struct arena_dalloc_bin_locked_info_s arena_dalloc_bin_locked_info_t;
struct arena_dalloc_bin_locked_info_s {
	div_info_t div_info;
	uint32_t nregs;
	uint64_t ndalloc;
};

JEMALLOC_ALWAYS_INLINE size_t
arena_slab_regind(arena_dalloc_bin_locked_info_t *info, szind_t binind,
    edata_t *slab, const void *ptr) {
	size_t regind = arena_slab_regind_impl(&info->div_info, binind, slab, ptr);
	return regind;
}

JEMALLOC_ALWAYS_INLINE void
arena_dalloc_bin_locked_begin(arena_dalloc_bin_locked_info_t *info,
    szind_t binind) {
	info->div_info = arena_binind_div_info[binind];
	info->nregs = bin_infos[binind].nregs;
	info->ndalloc = 0;
}

/*
 * Does the deallocation work associated with freeing a single pointer (a
 * "step") in between a arena_dalloc_bin_locked begin and end call.
 *
 * Returns true if arena_slab_dalloc must be called on slab.  Doesn't do
 * stats updates, which happen during finish (this lets running counts get left
 * in a register).
 */
JEMALLOC_ALWAYS_INLINE void
arena_dalloc_bin_locked_step(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    arena_dalloc_bin_locked_info_t *info, szind_t binind, edata_t *slab,
    void *ptr, edata_t **dalloc_slabs, unsigned ndalloc_slabs,
    unsigned *dalloc_slabs_count, edata_list_active_t *dalloc_slabs_extra) {
	const bin_info_t *bin_info = &bin_infos[binind];
	size_t regind = arena_slab_regind(info, binind, slab, ptr);
	slab_data_t *slab_data = edata_slab_data_get(slab);

	assert(edata_nfree_get(slab) < bin_info->nregs);
	/* Freeing an unallocated pointer can cause assertion failure. */
	assert(bitmap_get(slab_data->bitmap, &bin_info->bitmap_info, regind));

	bitmap_unset(slab_data->bitmap, &bin_info->bitmap_info, regind);
	edata_nfree_inc(slab);

	if (config_stats) {
		info->ndalloc++;
	}

	unsigned nfree = edata_nfree_get(slab);
	if (nfree == bin_info->nregs) {
		arena_dalloc_bin_locked_handle_newly_empty(tsdn, arena, slab,
		    bin);

		if (*dalloc_slabs_count < ndalloc_slabs) {
			dalloc_slabs[*dalloc_slabs_count] = slab;
			(*dalloc_slabs_count)++;
		} else {
			edata_list_active_append(dalloc_slabs_extra, slab);
		}
	} else if (nfree == 1 && slab != bin->slabcur) {
		arena_dalloc_bin_locked_handle_newly_nonempty(tsdn, arena, slab,
		    bin);
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_dalloc_bin_locked_finish(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    arena_dalloc_bin_locked_info_t *info) {
	if (config_stats) {
		bin->stats.ndalloc += info->ndalloc;
		assert(bin->stats.curregs >= (size_t)info->ndalloc);
		bin->stats.curregs -= (size_t)info->ndalloc;
	}
}

JEMALLOC_ALWAYS_INLINE void
arena_bin_flush_batch_impl(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    arena_dalloc_bin_locked_info_t *dalloc_bin_info, unsigned binind,
    edata_t **dalloc_slabs, unsigned ndalloc_slabs, unsigned *dalloc_count,
    edata_list_active_t *dalloc_slabs_extra) {
	assert(binind < bin_info_nbatched_sizes);
	bin_with_batch_t *batched_bin = (bin_with_batch_t *)bin;
	size_t nelems_to_pop = batcher_pop_begin(tsdn,
	    &batched_bin->remote_frees);

	bin_batching_test_mid_pop(nelems_to_pop);
	if (nelems_to_pop == BATCHER_NO_IDX) {
		malloc_mutex_assert_not_owner(tsdn,
		    &batched_bin->remote_frees.mtx);
		return;
	} else {
		malloc_mutex_assert_owner(tsdn,
		    &batched_bin->remote_frees.mtx);
	}

	size_t npushes = batcher_pop_get_pushes(tsdn,
	    &batched_bin->remote_frees);
	bin_remote_free_data_t remote_free_data[BIN_REMOTE_FREE_ELEMS_MAX];
	for (size_t i = 0; i < nelems_to_pop; i++) {
		remote_free_data[i] = batched_bin->remote_free_data[i];
	}
	batcher_pop_end(tsdn, &batched_bin->remote_frees);

	for (size_t i = 0; i < nelems_to_pop; i++) {
		arena_dalloc_bin_locked_step(tsdn, arena, bin, dalloc_bin_info,
		    binind, remote_free_data[i].slab, remote_free_data[i].ptr,
		    dalloc_slabs, ndalloc_slabs, dalloc_count,
		    dalloc_slabs_extra);
	}

	bin->stats.batch_pops++;
	bin->stats.batch_pushes += npushes;
	bin->stats.batch_pushed_elems += nelems_to_pop;
}

typedef struct arena_bin_flush_batch_state_s arena_bin_flush_batch_state_t;
struct arena_bin_flush_batch_state_s {
	arena_dalloc_bin_locked_info_t info;

	/*
	 * Bin batching is subtle in that there are unusual edge cases in which
	 * it can trigger the deallocation of more slabs than there were items
	 * flushed (say, if every original deallocation triggered a slab
	 * deallocation, and so did every batched one).  So we keep a small
	 * backup array for any "extra" slabs, as well as a a list to allow a
	 * dynamic number of ones exceeding that array.
	 */
	edata_t *dalloc_slabs[8];
	unsigned dalloc_slab_count;
	edata_list_active_t dalloc_slabs_extra;
};

JEMALLOC_ALWAYS_INLINE unsigned
arena_bin_batch_get_ndalloc_slabs(unsigned preallocated_slabs) {
	if (preallocated_slabs > bin_batching_test_ndalloc_slabs_max) {
		return bin_batching_test_ndalloc_slabs_max;
	}
	return preallocated_slabs;
}

JEMALLOC_ALWAYS_INLINE void
arena_bin_flush_batch_after_lock(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    unsigned binind, arena_bin_flush_batch_state_t *state) {
	if (binind >= bin_info_nbatched_sizes) {
		return;
	}

	arena_dalloc_bin_locked_begin(&state->info, binind);
	state->dalloc_slab_count = 0;
	edata_list_active_init(&state->dalloc_slabs_extra);

	unsigned preallocated_slabs = (unsigned)(sizeof(state->dalloc_slabs)
	    / sizeof(state->dalloc_slabs[0]));
	unsigned ndalloc_slabs = arena_bin_batch_get_ndalloc_slabs(
	    preallocated_slabs);

	arena_bin_flush_batch_impl(tsdn, arena, bin, &state->info, binind,
	    state->dalloc_slabs, ndalloc_slabs,
	    &state->dalloc_slab_count, &state->dalloc_slabs_extra);
}

JEMALLOC_ALWAYS_INLINE void
arena_bin_flush_batch_before_unlock(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    unsigned binind, arena_bin_flush_batch_state_t *state) {
	if (binind >= bin_info_nbatched_sizes) {
		return;
	}

	arena_dalloc_bin_locked_finish(tsdn, arena, bin, &state->info);
}

static inline bool
arena_bin_has_batch(szind_t binind) {
	return binind < bin_info_nbatched_sizes;
}

JEMALLOC_ALWAYS_INLINE void
arena_bin_flush_batch_after_unlock(tsdn_t *tsdn, arena_t *arena, bin_t *bin,
    unsigned binind, arena_bin_flush_batch_state_t *state) {
	if (!arena_bin_has_batch(binind)) {
		return;
	}
	/*
	 * The initialization of dalloc_slabs_extra is guarded by an
	 * arena_bin_has_batch check higher up the stack.  But the clang
	 * analyzer forgets this down the stack, triggering a spurious error
	 * reported here.
	 */
	JEMALLOC_CLANG_ANALYZER_SUPPRESS {
		bin_batching_test_after_unlock(state->dalloc_slab_count,
		    edata_list_active_empty(&state->dalloc_slabs_extra));
	}
	for (unsigned i = 0; i < state->dalloc_slab_count; i++) {
		edata_t *slab = state->dalloc_slabs[i];
		arena_slab_dalloc(tsdn, arena_get_from_edata(slab), slab);
	}
	while (!edata_list_active_empty(&state->dalloc_slabs_extra)) {
		edata_t *slab = edata_list_active_first(
		    &state->dalloc_slabs_extra);
		edata_list_active_remove(&state->dalloc_slabs_extra, slab);
		arena_slab_dalloc(tsdn, arena_get_from_edata(slab), slab);
	}
}

static inline bin_t *
arena_get_bin(arena_t *arena, szind_t binind, unsigned binshard) {
	bin_t *shard0 = (bin_t *)((byte_t *)arena + arena_bin_offsets[binind]);
	bin_t *ret;
	if (arena_bin_has_batch(binind)) {
		ret = (bin_t *)((bin_with_batch_t *)shard0 + binshard);
	} else {
		ret = shard0 + binshard;
	}
	assert(binind >= SC_NBINS - 1
	    || (uintptr_t)ret < (uintptr_t)arena
	    + arena_bin_offsets[binind + 1]);

	return ret;
}

#endif /* JEMALLOC_INTERNAL_ARENA_INLINES_B_H */
