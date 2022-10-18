#include "arraylist.hpp"

#include <limits.h>

#ifndef SIZE_T_MAX
#if SIZEOF_SIZE_T == SIZEOF_INT
#define SIZE_T_MAX UINT_MAX
#elif SIZEOF_SIZE_T == SIZEOF_LONG
#define SIZE_T_MAX ULONG_MAX
#elif SIZEOF_SIZE_T == SIZEOF_LONG_LONG
#define SIZE_T_MAX ULLONG_MAX
#else
#error Unable to determine size of size_t
#endif
#endif

namespace json {

static int array_list_expand_internal(struct array_list *arr, size_t max) {
	void *t;
	size_t new_size;

	if (max < arr->size)
		return 0;
	/* Avoid undefined behaviour on size_t overflow */
	if (arr->size >= SIZE_T_MAX / 2)
		new_size = max;
	else {
		new_size = arr->size << 1;
		if (new_size < max)
			new_size = max;
	}
	if (new_size > (~((size_t)0)) / sizeof(void *))
		return -1;
	if (!(t = realloc(arr->array, new_size * sizeof(void *))))
		return -1;
	arr->array = (void **)t;
	arr->size = new_size;
	return 0;
}

void *array_list_get_idx(struct array_list *arr, size_t i) {
	if (i >= arr->length)
		return NULL;
	return arr->array[i];
}

size_t array_list_length(struct array_list *arr) {
	return arr->length;
}

extern void array_list_free(struct array_list *arr) {
	size_t i;
	for (i = 0; i < arr->length; i++)
		if (arr->array[i])
			arr->free_fn(arr->array[i]);
	free(arr->array);
	free(arr);
}

int array_list_shrink(struct array_list *arr, size_t empty_slots) {
	void *t;
	size_t new_size;

	if (empty_slots >= SIZE_T_MAX / sizeof(void *) - arr->length)
		return -1;
	new_size = arr->length + empty_slots;
	if (new_size == arr->size)
		return 0;
	if (new_size > arr->size)
		return array_list_expand_internal(arr, new_size);
	if (new_size == 0)
		new_size = 1;

	if (!(t = realloc(arr->array, new_size * sizeof(void *))))
		return -1;
	arr->array = (void **)t;
	arr->size = new_size;
	return 0;
}

int array_list_add(struct array_list *arr, void *data) {
	/* Repeat some of array_list_put_idx() so we can skip several
	   checks that we know are unnecessary when appending at the end
	 */
	size_t idx = arr->length;
	if (idx > SIZE_T_MAX - 1)
		return -1;
	if (array_list_expand_internal(arr, idx + 1))
		return -1;
	arr->array[idx] = data;
	arr->length++;
	return 0;
}

struct array_list *array_list_new2(array_list_free_fn *free_fn, int initial_size) {
	struct array_list *arr;

	if (initial_size < 0 || (size_t)initial_size >= SIZE_T_MAX / sizeof(void *))
		return NULL;
	arr = (struct array_list *)malloc(sizeof(struct array_list));
	if (!arr)
		return NULL;
	arr->size = initial_size;
	arr->length = 0;
	arr->free_fn = free_fn;
	if (!(arr->array = (void **)malloc(arr->size * sizeof(void *)))) {
		free(arr);
		return NULL;
	}
	return arr;
}

} // namespace json
