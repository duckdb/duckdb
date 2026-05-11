#ifndef JEMALLOC_INTERNAL_TCACHE_INLINES_H
#define JEMALLOC_INTERNAL_TCACHE_INLINES_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/arena_externs.h"
#include "jemalloc/internal/bin.h"
#include "jemalloc/internal/jemalloc_internal_inlines_b.h"
#include "jemalloc/internal/jemalloc_internal_types.h"
#include "jemalloc/internal/large_externs.h"
#include "jemalloc/internal/san.h"
#include "jemalloc/internal/sc.h"
#include "jemalloc/internal/sz.h"
#include "jemalloc/internal/tcache_externs.h"
#include "jemalloc/internal/util.h"

static inline bool
tcache_enabled_get(tsd_t *tsd) {
	return tsd_tcache_enabled_get(tsd);
}

static inline unsigned
tcache_nbins_get(tcache_slow_t *tcache_slow) {
	assert(tcache_slow != NULL);
	unsigned nbins = tcache_slow->tcache_nbins;
	assert(nbins <= TCACHE_NBINS_MAX);
	return nbins;
}

static inline size_t
tcache_max_get(tcache_slow_t *tcache_slow) {
	assert(tcache_slow != NULL);
	size_t tcache_max = sz_index2size(tcache_nbins_get(tcache_slow) - 1);
	assert(tcache_max <= TCACHE_MAXCLASS_LIMIT);
	return tcache_max;
}

static inline void
tcache_max_set(tcache_slow_t *tcache_slow, size_t tcache_max) {
	assert(tcache_slow != NULL);
	assert(tcache_max <= TCACHE_MAXCLASS_LIMIT);
	tcache_slow->tcache_nbins = sz_size2index(tcache_max) + 1;
}

static inline void
tcache_bin_settings_backup(tcache_t *tcache,
    cache_bin_info_t tcache_bin_info[TCACHE_NBINS_MAX]) {
	for (unsigned i = 0; i < TCACHE_NBINS_MAX; i++) {
		cache_bin_info_init(&tcache_bin_info[i],
		    cache_bin_ncached_max_get_unsafe(&tcache->bins[i]));
	}
}

JEMALLOC_ALWAYS_INLINE bool
tcache_bin_disabled(szind_t ind, cache_bin_t *bin,
    tcache_slow_t *tcache_slow) {
	assert(bin != NULL);
	assert(ind < TCACHE_NBINS_MAX);
	bool disabled = cache_bin_disabled(bin);

	/*
	 * If a bin's ind >= nbins or ncached_max == 0, it must be disabled.
	 * However, when ind < nbins, it could be either enabled
	 * (ncached_max > 0) or disabled (ncached_max == 0). Similarly, when
	 * ncached_max > 0, it could be either enabled (ind < nbins) or
	 * disabled (ind >= nbins).  Thus, if a bin is disabled, it has either
	 * ind >= nbins or ncached_max == 0.  If a bin is enabled, it has
	 * ind < nbins and ncached_max > 0.
	 */
	unsigned nbins = tcache_nbins_get(tcache_slow);
	cache_bin_sz_t ncached_max = cache_bin_ncached_max_get_unsafe(bin);
	if (ind >= nbins) {
		assert(disabled);
	} else {
		assert(!disabled || ncached_max == 0);
	}
	if (ncached_max == 0) {
		assert(disabled);
	} else {
		assert(!disabled || ind >= nbins);
	}
	if (disabled) {
		assert(ind >= nbins || ncached_max == 0);
	} else {
		assert(ind < nbins && ncached_max > 0);
	}

	return disabled;
}

JEMALLOC_ALWAYS_INLINE void *
tcache_alloc_small(tsd_t *tsd, arena_t *arena, tcache_t *tcache,
    size_t size, szind_t binind, bool zero, bool slow_path) {
	void *ret;
	bool tcache_success;

	assert(binind < SC_NBINS);
	cache_bin_t *bin = &tcache->bins[binind];
	ret = cache_bin_alloc(bin, &tcache_success);
	assert(tcache_success == (ret != NULL));
	if (unlikely(!tcache_success)) {
		bool tcache_hard_success;
		arena = arena_choose(tsd, arena);
		if (unlikely(arena == NULL)) {
			return NULL;
		}
		if (unlikely(tcache_bin_disabled(binind, bin,
		    tcache->tcache_slow))) {
			/* stats and zero are handled directly by the arena. */
			return arena_malloc_hard(tsd_tsdn(tsd), arena, size,
			    binind, zero, /* slab */ true);
		}
		tcache_bin_flush_stashed(tsd, tcache, bin, binind,
		    /* is_small */ true);

		ret = tcache_alloc_small_hard(tsd_tsdn(tsd), arena, tcache,
		    bin, binind, &tcache_hard_success);
		if (tcache_hard_success == false) {
			return NULL;
		}
	}

	assert(ret);
	if (unlikely(zero)) {
		size_t usize = sz_index2size(binind);
		assert(tcache_salloc(tsd_tsdn(tsd), ret) == usize);
		memset(ret, 0, usize);
	}
	if (config_stats) {
		bin->tstats.nrequests++;
	}
	return ret;
}

JEMALLOC_ALWAYS_INLINE void *
tcache_alloc_large(tsd_t *tsd, arena_t *arena, tcache_t *tcache, size_t size,
    szind_t binind, bool zero, bool slow_path) {
	void *ret;
	bool tcache_success;

	cache_bin_t *bin = &tcache->bins[binind];
	assert(binind >= SC_NBINS &&
	    !tcache_bin_disabled(binind, bin, tcache->tcache_slow));
	ret = cache_bin_alloc(bin, &tcache_success);
	assert(tcache_success == (ret != NULL));
	if (unlikely(!tcache_success)) {
		/*
		 * Only allocate one large object at a time, because it's quite
		 * expensive to create one and not use it.
		 */
		arena = arena_choose(tsd, arena);
		if (unlikely(arena == NULL)) {
			return NULL;
		}
		tcache_bin_flush_stashed(tsd, tcache, bin, binind,
		    /* is_small */ false);

		ret = large_malloc(tsd_tsdn(tsd), arena, sz_s2u(size), zero);
		if (ret == NULL) {
			return NULL;
		}
	} else {
		if (unlikely(zero)) {
			size_t usize = sz_index2size(binind);
			assert(usize <= tcache_max_get(tcache->tcache_slow));
			memset(ret, 0, usize);
		}

		if (config_stats) {
			bin->tstats.nrequests++;
		}
	}

	return ret;
}

JEMALLOC_ALWAYS_INLINE void
tcache_dalloc_small(tsd_t *tsd, tcache_t *tcache, void *ptr, szind_t binind,
    bool slow_path) {
	assert(tcache_salloc(tsd_tsdn(tsd), ptr) <= SC_SMALL_MAXCLASS);

	cache_bin_t *bin = &tcache->bins[binind];
	/*
	 * Not marking the branch unlikely because this is past free_fastpath()
	 * (which handles the most common cases), i.e. at this point it's often
	 * uncommon cases.
	 */
	if (cache_bin_nonfast_aligned(ptr)) {
		/* Junk unconditionally, even if bin is full. */
		san_junk_ptr(ptr, sz_index2size(binind));
		if (cache_bin_stash(bin, ptr)) {
			return;
		}
		assert(cache_bin_full(bin));
		/* Bin full; fall through into the flush branch. */
	}

	if (unlikely(!cache_bin_dalloc_easy(bin, ptr))) {
		if (unlikely(tcache_bin_disabled(binind, bin,
		    tcache->tcache_slow))) {
			arena_dalloc_small(tsd_tsdn(tsd), ptr);
			return;
		}
		cache_bin_sz_t max = cache_bin_ncached_max_get(bin);
		unsigned remain = max >> opt_lg_tcache_flush_small_div;
		tcache_bin_flush_small(tsd, tcache, bin, binind, remain);
		bool ret = cache_bin_dalloc_easy(bin, ptr);
		assert(ret);
	}
}

JEMALLOC_ALWAYS_INLINE void
tcache_dalloc_large(tsd_t *tsd, tcache_t *tcache, void *ptr, szind_t binind,
    bool slow_path) {

	assert(tcache_salloc(tsd_tsdn(tsd), ptr) > SC_SMALL_MAXCLASS);
	assert(tcache_salloc(tsd_tsdn(tsd), ptr) <=
	    tcache_max_get(tcache->tcache_slow));
	assert(!tcache_bin_disabled(binind, &tcache->bins[binind],
	    tcache->tcache_slow));

	cache_bin_t *bin = &tcache->bins[binind];
	if (unlikely(!cache_bin_dalloc_easy(bin, ptr))) {
		unsigned remain = cache_bin_ncached_max_get(bin) >>
		    opt_lg_tcache_flush_large_div;
		tcache_bin_flush_large(tsd, tcache, bin, binind, remain);
		bool ret = cache_bin_dalloc_easy(bin, ptr);
		assert(ret);
	}
}

JEMALLOC_ALWAYS_INLINE tcache_t *
tcaches_get(tsd_t *tsd, unsigned ind) {
	tcaches_t *elm = &tcaches[ind];
	if (unlikely(elm->tcache == NULL)) {
		malloc_printf("<jemalloc>: invalid tcache id (%u).\n", ind);
		abort();
	} else if (unlikely(elm->tcache == TCACHES_ELM_NEED_REINIT)) {
		elm->tcache = tcache_create_explicit(tsd);
	}
	return elm->tcache;
}

#endif /* JEMALLOC_INTERNAL_TCACHE_INLINES_H */
