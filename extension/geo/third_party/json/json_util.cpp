#include "json_util.hpp"

#include <cstddef>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

namespace json {

int json_parse_int64(const char *buf, int64_t *retval) {
	char *end = NULL;
	int64_t val;

	errno = 0;
	val = strtoll(buf, &end, 10);
	if (end != buf)
		*retval = val;
	return ((val == 0 && errno != 0) || (end == buf)) ? 1 : 0;
}

int json_parse_uint64(const char *buf, uint64_t *retval) {
	char *end = NULL;
	uint64_t val;

	errno = 0;
	while (*buf == ' ')
		buf++;
	if (*buf == '-')
		return 1; /* error: uint cannot be negative */

	val = strtoull(buf, &end, 10);
	if (end != buf)
		*retval = val;
	return ((val == 0 && errno != 0) || (end == buf)) ? 1 : 0;
}

} // namespace json
