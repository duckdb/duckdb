#ifndef JEMALLOC_INTERNAL_INLINES_C_H
#define JEMALLOC_INTERNAL_INLINES_C_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/arena_externs.h"
#include "jemalloc/internal/arena_inlines_b.h"
#include "jemalloc/internal/emap.h"
#include "jemalloc/internal/hook.h"
#include "jemalloc/internal/jemalloc_internal_types.h"
#include "jemalloc/internal/log.h"
#include "jemalloc/internal/sz.h"
#include "jemalloc/internal/thread_event.h"
#include "jemalloc/internal/witness.h"

/*
 * These correspond to the macros in jemalloc/jemalloc_macros.h.  Broadly, we
 * should have one constant here per magic value there.  Note however that the
 * representations need not be related.
 */
#define TCACHE_IND_NONE ((unsigned)-1)
#define TCACHE_IND_AUTOMATIC ((unsigned)-2)
#define ARENA_IND_AUTOMATIC ((unsigned)-1)

/*
 * Translating the names of the 'i' functions:
 *   Abbreviations used in the first part of the function name (before
 *   alloc/dalloc) describe what that function accomplishes:
 *     a: arena (query)
 *     s: size (query, or sized deallocation)
 *     e: extent (query)
 *     p: aligned (allocates)
 *     vs: size (query, without knowing that the pointer is into the heap)
 *     r: rallocx implementation
 *     x: xallocx implementation
 *   Abbreviations used in the second part of the function name (after
 *   alloc/dalloc) describe the arguments it takes
 *     z: whether to return zeroed memory
 *     t: accepts a tcache_t * parameter
 *     m: accepts an arena_t * parameter
 */

JEMALLOC_ALWAYS_INLINE arena_t *
iaalloc(tsdn_t *tsdn, const void *ptr) {
	assert(ptr != NULL);

	return arena_aalloc(tsdn, ptr);
}

JEMALLOC_ALWAYS_INLINE size_t
isalloc(tsdn_t *tsdn, const void *ptr) {
	assert(ptr != NULL);

	return arena_salloc(tsdn, ptr);
}

JEMALLOC_ALWAYS_INLINE void *
iallocztm_explicit_slab(tsdn_t *tsdn, size_t size, szind_t ind, bool zero,
    bool slab, tcache_t *tcache, bool is_internal, arena_t *arena,
    bool slow_path) {
	void *ret;

	assert(!slab || sz_can_use_slab(size)); /* slab && large is illegal */
	assert(!is_internal || tcache == NULL);
	assert(!is_internal || arena == NULL || arena_is_auto(arena));
	if (!tsdn_null(tsdn) && tsd_reentrancy_level_get(tsdn_tsd(tsdn)) == 0) {
		witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
		    WITNESS_RANK_CORE, 0);
	}

	ret = arena_malloc(tsdn, arena, size, ind, zero, slab, tcache, slow_path);
	if (config_stats && is_internal && likely(ret != NULL)) {
		arena_internal_add(iaalloc(tsdn, ret), isalloc(tsdn, ret));
	}
	return ret;
}

JEMALLOC_ALWAYS_INLINE void *
iallocztm(tsdn_t *tsdn, size_t size, szind_t ind, bool zero, tcache_t *tcache,
    bool is_internal, arena_t *arena, bool slow_path) {
	bool slab = sz_can_use_slab(size);
	return iallocztm_explicit_slab(tsdn, size, ind, zero, slab, tcache,
	    is_internal, arena, slow_path);
}

JEMALLOC_ALWAYS_INLINE void *
ialloc(tsd_t *tsd, size_t size, szind_t ind, bool zero, bool slow_path) {
	return iallocztm(tsd_tsdn(tsd), size, ind, zero, tcache_get(tsd), false,
	    NULL, slow_path);
}

JEMALLOC_ALWAYS_INLINE void *
ipallocztm_explicit_slab(tsdn_t *tsdn, size_t usize, size_t alignment, bool zero,
    bool slab, tcache_t *tcache, bool is_internal, arena_t *arena) {
	void *ret;

	assert(!slab || sz_can_use_slab(usize)); /* slab && large is illegal */
	assert(usize != 0);
	assert(usize == sz_sa2u(usize, alignment));
	assert(!is_internal || tcache == NULL);
	assert(!is_internal || arena == NULL || arena_is_auto(arena));
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);

	ret = arena_palloc(tsdn, arena, usize, alignment, zero, slab, tcache);
	assert(ALIGNMENT_ADDR2BASE(ret, alignment) == ret);
	if (config_stats && is_internal && likely(ret != NULL)) {
		arena_internal_add(iaalloc(tsdn, ret), isalloc(tsdn, ret));
	}
	return ret;
}

JEMALLOC_ALWAYS_INLINE void *
ipallocztm(tsdn_t *tsdn, size_t usize, size_t alignment, bool zero,
    tcache_t *tcache, bool is_internal, arena_t *arena) {
	return ipallocztm_explicit_slab(tsdn, usize, alignment, zero,
	    sz_can_use_slab(usize), tcache, is_internal, arena);
}

JEMALLOC_ALWAYS_INLINE void *
ipalloct(tsdn_t *tsdn, size_t usize, size_t alignment, bool zero,
    tcache_t *tcache, arena_t *arena) {
	return ipallocztm(tsdn, usize, alignment, zero, tcache, false, arena);
}

JEMALLOC_ALWAYS_INLINE void *
ipalloct_explicit_slab(tsdn_t *tsdn, size_t usize, size_t alignment,
    bool zero, bool slab, tcache_t *tcache, arena_t *arena) {
	return ipallocztm_explicit_slab(tsdn, usize, alignment, zero, slab,
	    tcache, false, arena);
}

JEMALLOC_ALWAYS_INLINE void *
ipalloc(tsd_t *tsd, size_t usize, size_t alignment, bool zero) {
	return ipallocztm(tsd_tsdn(tsd), usize, alignment, zero,
	    tcache_get(tsd), false, NULL);
}

JEMALLOC_ALWAYS_INLINE size_t
ivsalloc(tsdn_t *tsdn, const void *ptr) {
	return arena_vsalloc(tsdn, ptr);
}

JEMALLOC_ALWAYS_INLINE void
idalloctm(tsdn_t *tsdn, void *ptr, tcache_t *tcache,
    emap_alloc_ctx_t *alloc_ctx, bool is_internal, bool slow_path) {
	assert(ptr != NULL);
	assert(!is_internal || tcache == NULL);
	assert(!is_internal || arena_is_auto(iaalloc(tsdn, ptr)));
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);
	if (config_stats && is_internal) {
		arena_internal_sub(iaalloc(tsdn, ptr), isalloc(tsdn, ptr));
	}
	if (!is_internal && !tsdn_null(tsdn) &&
	    tsd_reentrancy_level_get(tsdn_tsd(tsdn)) != 0) {
		assert(tcache == NULL);
	}
	arena_dalloc(tsdn, ptr, tcache, alloc_ctx, slow_path);
}

JEMALLOC_ALWAYS_INLINE void
idalloc(tsd_t *tsd, void *ptr) {
	idalloctm(tsd_tsdn(tsd), ptr, tcache_get(tsd), NULL, false, true);
}

JEMALLOC_ALWAYS_INLINE void
isdalloct(tsdn_t *tsdn, void *ptr, size_t size, tcache_t *tcache,
    emap_alloc_ctx_t *alloc_ctx, bool slow_path) {
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);
	arena_sdalloc(tsdn, ptr, size, tcache, alloc_ctx, slow_path);
}

JEMALLOC_ALWAYS_INLINE void *
iralloct_realign(tsdn_t *tsdn, void *ptr, size_t oldsize, size_t size,
    size_t alignment, bool zero, bool slab, tcache_t *tcache, arena_t *arena,
    hook_ralloc_args_t *hook_args) {
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);
	void *p;
	size_t usize, copysize;

	usize = sz_sa2u(size, alignment);
	if (unlikely(usize == 0 || usize > SC_LARGE_MAXCLASS)) {
		return NULL;
	}
	p = ipalloct_explicit_slab(tsdn, usize, alignment, zero, slab,
	    tcache, arena);
	if (p == NULL) {
		return NULL;
	}
	/*
	 * Copy at most size bytes (not size+extra), since the caller has no
	 * expectation that the extra bytes will be reliably preserved.
	 */
	copysize = (size < oldsize) ? size : oldsize;
	memcpy(p, ptr, copysize);
	hook_invoke_alloc(hook_args->is_realloc
	    ? hook_alloc_realloc : hook_alloc_rallocx, p, (uintptr_t)p,
	    hook_args->args);
	hook_invoke_dalloc(hook_args->is_realloc
	    ? hook_dalloc_realloc : hook_dalloc_rallocx, ptr, hook_args->args);
	isdalloct(tsdn, ptr, oldsize, tcache, NULL, true);
	return p;
}

/*
 * is_realloc threads through the knowledge of whether or not this call comes
 * from je_realloc (as opposed to je_rallocx); this ensures that we pass the
 * correct entry point into any hooks.
 * Note that these functions are all force-inlined, so no actual bool gets
 * passed-around anywhere.
 */
JEMALLOC_ALWAYS_INLINE void *
iralloct_explicit_slab(tsdn_t *tsdn, void *ptr, size_t oldsize, size_t size,
    size_t alignment, bool zero, bool slab, tcache_t *tcache, arena_t *arena,
    hook_ralloc_args_t *hook_args)
{
	assert(ptr != NULL);
	assert(size != 0);
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);

	if (alignment != 0 && ((uintptr_t)ptr & ((uintptr_t)alignment-1))
	    != 0) {
		/*
		 * Existing object alignment is inadequate; allocate new space
		 * and copy.
		 */
		return iralloct_realign(tsdn, ptr, oldsize, size, alignment,
		    zero, slab, tcache, arena, hook_args);
	}

	return arena_ralloc(tsdn, arena, ptr, oldsize, size, alignment, zero,
	    slab, tcache, hook_args);
}

JEMALLOC_ALWAYS_INLINE void *
iralloct(tsdn_t *tsdn, void *ptr, size_t oldsize, size_t size, size_t alignment,
    size_t usize, bool zero, tcache_t *tcache, arena_t *arena,
    hook_ralloc_args_t *hook_args)
{
	bool slab = sz_can_use_slab(usize);
	return iralloct_explicit_slab(tsdn, ptr, oldsize, size, alignment, zero,
	    slab, tcache, arena, hook_args);
}

JEMALLOC_ALWAYS_INLINE void *
iralloc(tsd_t *tsd, void *ptr, size_t oldsize, size_t size, size_t alignment,
    size_t usize, bool zero, hook_ralloc_args_t *hook_args) {
	return iralloct(tsd_tsdn(tsd), ptr, oldsize, size, alignment, usize,
	    zero, tcache_get(tsd), NULL, hook_args);
}

JEMALLOC_ALWAYS_INLINE bool
ixalloc(tsdn_t *tsdn, void *ptr, size_t oldsize, size_t size, size_t extra,
    size_t alignment, bool zero, size_t *newsize) {
	assert(ptr != NULL);
	assert(size != 0);
	witness_assert_depth_to_rank(tsdn_witness_tsdp_get(tsdn),
	    WITNESS_RANK_CORE, 0);

	if (alignment != 0 && ((uintptr_t)ptr & ((uintptr_t)alignment-1))
	    != 0) {
		/* Existing object alignment is inadequate. */
		*newsize = oldsize;
		return true;
	}

	return arena_ralloc_no_move(tsdn, ptr, oldsize, size, extra, zero,
	    newsize);
}

JEMALLOC_ALWAYS_INLINE void
fastpath_success_finish(tsd_t *tsd, uint64_t allocated_after,
    cache_bin_t *bin, void *ret) {
	thread_allocated_set(tsd, allocated_after);
	if (config_stats) {
		bin->tstats.nrequests++;
	}
}

JEMALLOC_ALWAYS_INLINE bool
malloc_initialized(void) {
	return (malloc_init_state == malloc_init_initialized);
}

/*
 * malloc() fastpath.  Included here so that we can inline it into operator new;
 * function call overhead there is non-negligible as a fraction of total CPU in
 * allocation-heavy C++ programs.  We take the fallback alloc to allow malloc
 * (which can return NULL) to differ in its behavior from operator new (which
 * can't).  It matches the signature of malloc / operator new so that we can
 * tail-call the fallback allocator, allowing us to avoid setting up the call
 * frame in the common case.
 *
 * Fastpath assumes size <= SC_LOOKUP_MAXCLASS, and that we hit
 * tcache.  If either of these is false, we tail-call to the slowpath,
 * malloc_default().  Tail-calling is used to avoid any caller-saved
 * registers.
 *
 * fastpath supports ticker and profiling, both of which will also
 * tail-call to the slowpath if they fire.
 */
JEMALLOC_ALWAYS_INLINE void *
imalloc_fastpath(size_t size, void *(fallback_alloc)(size_t)) {
	if (tsd_get_allocates() && unlikely(!malloc_initialized())) {
		return fallback_alloc(size);
	}

	tsd_t *tsd = tsd_get(false);
	if (unlikely((size > SC_LOOKUP_MAXCLASS) || tsd == NULL)) {
		return fallback_alloc(size);
	}
	/*
	 * The code below till the branch checking the next_event threshold may
	 * execute before malloc_init(), in which case the threshold is 0 to
	 * trigger slow path and initialization.
	 *
	 * Note that when uninitialized, only the fast-path variants of the sz /
	 * tsd facilities may be called.
	 */
	szind_t ind;
	/*
	 * The thread_allocated counter in tsd serves as a general purpose
	 * accumulator for bytes of allocation to trigger different types of
	 * events.  usize is always needed to advance thread_allocated, though
	 * it's not always needed in the core allocation logic.
	 */
	size_t usize;
	sz_size2index_usize_fastpath(size, &ind, &usize);
	/* Fast path relies on size being a bin. */
	assert(ind < SC_NBINS);
	assert((SC_LOOKUP_MAXCLASS < SC_SMALL_MAXCLASS) &&
	    (size <= SC_SMALL_MAXCLASS));

	uint64_t allocated, threshold;
	te_malloc_fastpath_ctx(tsd, &allocated, &threshold);
	uint64_t allocated_after = allocated + usize;
	/*
	 * The ind and usize might be uninitialized (or partially) before
	 * malloc_init().  The assertions check for: 1) full correctness (usize
	 * & ind) when initialized; and 2) guaranteed slow-path (threshold == 0)
	 * when !initialized.
	 */
	if (!malloc_initialized()) {
		assert(threshold == 0);
	} else {
		assert(ind == sz_size2index(size));
		assert(usize > 0 && usize == sz_index2size(ind));
	}
	/*
	 * Check for events and tsd non-nominal (fast_threshold will be set to
	 * 0) in a single branch.
	 */
	if (unlikely(allocated_after >= threshold)) {
		return fallback_alloc(size);
	}
	assert(tsd_fast(tsd));

	tcache_t *tcache = tsd_tcachep_get(tsd);
	assert(tcache == tcache_get(tsd));
	cache_bin_t *bin = &tcache->bins[ind];
	/* Suppress spurious warning from static analysis */
	assert(bin != NULL);
	bool tcache_success;
	void *ret;

	/*
	 * We split up the code this way so that redundant low-water
	 * computation doesn't happen on the (more common) case in which we
	 * don't touch the low water mark.  The compiler won't do this
	 * duplication on its own.
	 */
	ret = cache_bin_alloc_easy(bin, &tcache_success);
	if (tcache_success) {
		fastpath_success_finish(tsd, allocated_after, bin, ret);
		return ret;
	}
	ret = cache_bin_alloc(bin, &tcache_success);
	if (tcache_success) {
		fastpath_success_finish(tsd, allocated_after, bin, ret);
		return ret;
	}

	return fallback_alloc(size);
}

JEMALLOC_ALWAYS_INLINE tcache_t *
tcache_get_from_ind(tsd_t *tsd, unsigned tcache_ind, bool slow, bool is_alloc) {
        tcache_t *tcache;
        if (tcache_ind == TCACHE_IND_AUTOMATIC) {
                if (likely(!slow)) {
                        /* Getting tcache ptr unconditionally. */
                        tcache = tsd_tcachep_get(tsd);
                        assert(tcache == tcache_get(tsd));
                } else if (is_alloc ||
                    likely(tsd_reentrancy_level_get(tsd) == 0)) {
                        tcache = tcache_get(tsd);
                } else {
                        tcache = NULL;
                }
        } else {
                /*
                 * Should not specify tcache on deallocation path when being
                 * reentrant.
                 */
                assert(is_alloc || tsd_reentrancy_level_get(tsd) == 0 ||
                    tsd_state_nocleanup(tsd));
                if (tcache_ind == TCACHE_IND_NONE) {
                        tcache = NULL;
                } else {
                        tcache = tcaches_get(tsd, tcache_ind);
                }
        }
        return tcache;
}

JEMALLOC_ALWAYS_INLINE bool
maybe_check_alloc_ctx(tsd_t *tsd, void *ptr, emap_alloc_ctx_t *alloc_ctx) {
        if (config_opt_size_checks) {
                emap_alloc_ctx_t dbg_ctx;
                emap_alloc_ctx_lookup(tsd_tsdn(tsd), &arena_emap_global, ptr,
                    &dbg_ctx);
                if (alloc_ctx->szind != dbg_ctx.szind) {
                        safety_check_fail_sized_dealloc(
                            /* current_dealloc */ true, ptr,
                            /* true_size */ sz_size2index(dbg_ctx.szind),
                            /* input_size */ sz_size2index(alloc_ctx->szind));
                        return true;
                }
                if (alloc_ctx->slab != dbg_ctx.slab) {
                        safety_check_fail(
                            "Internal heap corruption detected: "
                            "mismatch in slab bit");
                        return true;
                }
        }
        return false;
}

JEMALLOC_ALWAYS_INLINE bool
prof_sample_aligned(const void *ptr) {
	return ((uintptr_t)ptr & PROF_SAMPLE_ALIGNMENT_MASK) == 0;
}

JEMALLOC_ALWAYS_INLINE bool
free_fastpath_nonfast_aligned(void *ptr, bool check_prof) {
        /*
         * free_fastpath do not handle two uncommon cases: 1) sampled profiled
         * objects and 2) sampled junk & stash for use-after-free detection.
         * Both have special alignments which are used to escape the fastpath.
         *
         * prof_sample is page-aligned, which covers the UAF check when both
         * are enabled (the assertion below).  Avoiding redundant checks since
         * this is on the fastpath -- at most one runtime branch from this.
         */
        if (config_debug && cache_bin_nonfast_aligned(ptr)) {
                assert(prof_sample_aligned(ptr));
        }

        if (config_prof && check_prof) {
                /* When prof is enabled, the prof_sample alignment is enough. */
                if (prof_sample_aligned(ptr)) {
                        return true;
                } else {
                        return false;
                }
        }

        if (config_uaf_detection) {
                if (cache_bin_nonfast_aligned(ptr)) {
                        return true;
                } else {
                        return false;
                }
        }

        return false;
}

/* Returns whether or not the free attempt was successful. */
JEMALLOC_ALWAYS_INLINE
bool free_fastpath(void *ptr, size_t size, bool size_hint) {
        tsd_t *tsd = tsd_get(false);
        /* The branch gets optimized away unless tsd_get_allocates(). */
        if (unlikely(tsd == NULL)) {
                return false;
        }
        /*
         *  The tsd_fast() / initialized checks are folded into the branch
         *  testing (deallocated_after >= threshold) later in this function.
         *  The threshold will be set to 0 when !tsd_fast.
         */
        assert(tsd_fast(tsd) ||
            *tsd_thread_deallocated_next_event_fastp_get_unsafe(tsd) == 0);

        emap_alloc_ctx_t alloc_ctx;
        if (!size_hint) {
                bool err = emap_alloc_ctx_try_lookup_fast(tsd,
                    &arena_emap_global, ptr, &alloc_ctx);

                /* Note: profiled objects will have alloc_ctx.slab set */
                if (unlikely(err || !alloc_ctx.slab ||
                    free_fastpath_nonfast_aligned(ptr,
                    /* check_prof */ false))) {
                        return false;
                }
                assert(alloc_ctx.szind != SC_NSIZES);
        } else {
                /*
                 * Check for both sizes that are too large, and for sampled /
                 * special aligned objects.  The alignment check will also check
                 * for null ptr.
                 */
                if (unlikely(size > SC_LOOKUP_MAXCLASS ||
                    free_fastpath_nonfast_aligned(ptr,
                    /* check_prof */ true))) {
                        return false;
                }
                alloc_ctx.szind = sz_size2index_lookup(size);
                /* Max lookup class must be small. */
                assert(alloc_ctx.szind < SC_NBINS);
                /* This is a dead store, except when opt size checking is on. */
                alloc_ctx.slab = true;
        }
        /*
         * Currently the fastpath only handles small sizes.  The branch on
         * SC_LOOKUP_MAXCLASS makes sure of it.  This lets us avoid checking
         * tcache szind upper limit (i.e. tcache_max) as well.
         */
        assert(alloc_ctx.slab);

        uint64_t deallocated, threshold;
        te_free_fastpath_ctx(tsd, &deallocated, &threshold);

        size_t usize = sz_index2size(alloc_ctx.szind);
        uint64_t deallocated_after = deallocated + usize;
        /*
         * Check for events and tsd non-nominal (fast_threshold will be set to
         * 0) in a single branch.  Note that this handles the uninitialized case
         * as well (TSD init will be triggered on the non-fastpath).  Therefore
         * anything depends on a functional TSD (e.g. the alloc_ctx sanity check
         * below) needs to be after this branch.
         */
        if (unlikely(deallocated_after >= threshold)) {
                return false;
        }
        assert(tsd_fast(tsd));
        bool fail = maybe_check_alloc_ctx(tsd, ptr, &alloc_ctx);
        if (fail) {
                /* See the comment in isfree. */
                return true;
        }

        tcache_t *tcache = tcache_get_from_ind(tsd, TCACHE_IND_AUTOMATIC,
            /* slow */ false, /* is_alloc */ false);
        cache_bin_t *bin = &tcache->bins[alloc_ctx.szind];

        /*
         * If junking were enabled, this is where we would do it.  It's not
         * though, since we ensured above that we're on the fast path.  Assert
         * that to double-check.
         */
        assert(!opt_junk_free);

        if (!cache_bin_dalloc_easy(bin, ptr)) {
                return false;
        }

        *tsd_thread_deallocatedp_get(tsd) = deallocated_after;

        return true;
}

JEMALLOC_ALWAYS_INLINE void JEMALLOC_NOTHROW
je_sdallocx_noflags(void *ptr, size_t size) {
        if (!free_fastpath(ptr, size, true)) {
                sdallocx_default(ptr, size, 0);
        }
}

JEMALLOC_ALWAYS_INLINE void JEMALLOC_NOTHROW
je_sdallocx_impl(void *ptr, size_t size, int flags) {
        if (flags != 0 || !free_fastpath(ptr, size, true)) {
                sdallocx_default(ptr, size, flags);
        }
}

JEMALLOC_ALWAYS_INLINE void JEMALLOC_NOTHROW
je_free_impl(void *ptr) {
        if (!free_fastpath(ptr, 0, false)) {
                free_default(ptr);
        }
}

#endif /* JEMALLOC_INTERNAL_INLINES_C_H */
