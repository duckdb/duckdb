#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

void
exp_grow_init(exp_grow_t *exp_grow) {
	/*
	 * Enforce a minimal of 2M grow, which is convenient for the huge page
	 * use cases.  Avoid using HUGEPAGE as the value though, because on some
	 * platforms it can be very large (e.g. 512M on aarch64 w/ 64K pages).
	 */
	const size_t min_grow = (size_t)2 << 20;
	exp_grow->next = sz_psz2ind(min_grow);
	exp_grow->limit = sz_psz2ind(SC_LARGE_MAXCLASS);
}
