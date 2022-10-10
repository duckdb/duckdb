#pragma one

#include <stddef.h>
#include <stdlib.h>

namespace json {

#ifndef _json_c_arraylist_h_
#define _json_c_arraylist_h_

#define ARRAY_LIST_DEFAULT_SIZE 32

typedef void(array_list_free_fn)(void *data);

struct array_list {
	void **array;
	size_t length;
	size_t size;
	array_list_free_fn *free_fn;
};
typedef struct array_list array_list;

/**
 * Allocate an array_list of the desired size.
 *
 * If possible, the size should be chosen to closely match
 * the actual number of elements expected to be used.
 * If the exact size is unknown, there are tradeoffs to be made:
 * - too small - the array_list code will need to call realloc() more
 *   often (which might incur an additional memory copy).
 * - too large - will waste memory, but that can be mitigated
 *   by calling array_list_shrink() once the final size is known.
 *
 * @see array_list_shrink
 */
extern struct array_list *array_list_new2(array_list_free_fn *free_fn, int initial_size);

extern void *array_list_get_idx(struct array_list *al, size_t i);

extern size_t array_list_length(struct array_list *al);

extern void array_list_free(struct array_list *al);

/**
 * Shrink the array list to just enough to fit the number of elements in it,
 * plus empty_slots.
 */
extern int array_list_shrink(struct array_list *arr, size_t empty_slots);

extern int array_list_add(struct array_list *al, void *data);

#endif

} // namespace json
