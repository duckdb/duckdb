#pragma one

#include "json_types.hpp"

#include <cstddef>
#include <inttypes.h>

namespace json {

/* json object int type, support extension*/
typedef enum json_object_int_type { json_object_int_type_int64, json_object_int_type_uint64 } json_object_int_type;

struct json_object {
	enum json_type o_type;
	uint32_t _ref_count;
	json_object_to_json_string_fn *_to_json_string;
	struct printbuf *_pb;
	json_object_delete_fn *_user_delete;
	void *_userdata;
	// Actually longer, always malloc'd as some more-specific type.
	// The rest of a struct json_object_${o_type} follows
};

struct json_object_object {
	struct json_object base;
	struct lh_table *c_object;
};

struct json_object_array {
	struct json_object base;
	struct array_list *c_array;
};

struct json_object_boolean {
	struct json_object base;
	json_bool c_boolean;
};

struct json_object_double {
	struct json_object base;
	double c_double;
};

struct json_object_int {
	struct json_object base;
	enum json_object_int_type cint_type;
	union {
		int64_t c_int64;
		uint64_t c_uint64;
	} cint;
};

struct json_object_string {
	struct json_object base;
	std::ptrdiff_t len; // Signed b/c negative lengths indicate data is a pointer
	// Consider adding an "alloc" field here, if json_object_set_string calls
	// to expand the length of a string are common operations to perform.
	union {
		char idata[1]; // Immediate data.  Actually longer
		char *pdata;   // Only when len < 0
	} c_string;
};

extern const char *json_hex_chars;

} // namespace json
