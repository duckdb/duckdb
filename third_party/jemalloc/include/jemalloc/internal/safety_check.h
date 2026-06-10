#ifndef JEMALLOC_INTERNAL_SAFETY_CHECK_H
#define JEMALLOC_INTERNAL_SAFETY_CHECK_H

#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/assert.h"
#include "jemalloc/internal/pages.h"

#define SAFETY_CHECK_DOUBLE_FREE_MAX_SCAN_DEFAULT 32

void safety_check_fail_sized_dealloc(bool current_dealloc, const void *ptr,
    size_t true_size, size_t input_size);
void safety_check_fail(const char *format, ...);

typedef void (*safety_check_abort_hook_t)(const char *message);

/* Can set to NULL for a default. */
void safety_check_set_abort(safety_check_abort_hook_t abort_fn);

#define REDZONE_SIZE ((size_t) 32)
#define REDZONE_FILL_VALUE 0xBC

/*
 * Normally the redzone extends `REDZONE_SIZE` bytes beyond the end of
 * the allocation. However, we don't let the redzone extend onto another
 * OS page because this would impose additional overhead if that page was
 * not already resident in memory.
 */
JEMALLOC_ALWAYS_INLINE const unsigned char *
compute_redzone_end(const void *_ptr, size_t usize, size_t bumped_usize) {
	const unsigned char *ptr = (const unsigned char *) _ptr;
	const unsigned char *redzone_end = usize + REDZONE_SIZE < bumped_usize ?
	    &ptr[usize + REDZONE_SIZE] : &ptr[bumped_usize];
	const unsigned char *page_end = (const unsigned char *)
	    ALIGNMENT_ADDR2CEILING(&ptr[usize], os_page);
	return redzone_end < page_end ? redzone_end : page_end;
}

JEMALLOC_ALWAYS_INLINE void
safety_check_set_redzone(void *ptr, size_t usize, size_t bumped_usize) {
	assert(usize <= bumped_usize);
	const unsigned char *redzone_end =
		compute_redzone_end(ptr, usize, bumped_usize);
	for (unsigned char *curr = &((unsigned char *)ptr)[usize];
	     curr < redzone_end; curr++) {
		*curr = REDZONE_FILL_VALUE;
	}
}

JEMALLOC_ALWAYS_INLINE void
safety_check_verify_redzone(const void *ptr, size_t usize, size_t bumped_usize)
{
	const unsigned char *redzone_end =
		compute_redzone_end(ptr, usize, bumped_usize);
	for (const unsigned char *curr= &((const unsigned char *)ptr)[usize];
	     curr < redzone_end; curr++) {
		if (unlikely(*curr != REDZONE_FILL_VALUE)) {
			safety_check_fail("Use after free error\n");
		}
	}
}

#undef REDZONE_SIZE
#undef REDZONE_FILL_VALUE

#endif /*JEMALLOC_INTERNAL_SAFETY_CHECK_H */
