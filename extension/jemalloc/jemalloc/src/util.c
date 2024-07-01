#include "jemalloc/internal/jemalloc_preamble.h"
#include "jemalloc/internal/jemalloc_internal_includes.h"

#include "jemalloc/internal/util.h"

/* Reads the next size pair in a multi-sized option. */
bool
multi_setting_parse_next(const char **setting_segment_cur, size_t *len_left,
    size_t *key_start, size_t *key_end, size_t *value) {
	const char *cur = *setting_segment_cur;
	char *end;
	uintmax_t um;

	set_errno(0);

	/* First number, then '-' */
	um = malloc_strtoumax(cur, &end, 0);
	if (get_errno() != 0 || *end != '-') {
		return true;
	}
	*key_start = (size_t)um;
	cur = end + 1;

	/* Second number, then ':' */
	um = malloc_strtoumax(cur, &end, 0);
	if (get_errno() != 0 || *end != ':') {
		return true;
	}
	*key_end = (size_t)um;
	cur = end + 1;

	/* Last number */
	um = malloc_strtoumax(cur, &end, 0);
	if (get_errno() != 0) {
		return true;
	}
	*value = (size_t)um;

	/* Consume the separator if there is one. */
	if (*end == '|') {
		end++;
	}

	*len_left -= end - *setting_segment_cur;
	*setting_segment_cur = end;

	return false;
}

