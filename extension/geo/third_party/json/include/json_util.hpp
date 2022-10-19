#pragma one

#include "duckdb.hpp"

#include <inttypes.h>

namespace json {

#define json_min(a, b) ((a) < (b) ? (a) : (b))

/* these parsing helpers return zero on success */
extern int json_parse_int64(const char *buf, int64_t *retval);
extern int json_parse_uint64(const char *buf, uint64_t *retval);

} // namespace json
