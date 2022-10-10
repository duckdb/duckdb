#pragma one

#include <cassert>
#include <errno.h>
#include <math.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

namespace json {

typedef int json_bool;

/**
 * A structure to use with json_object_object_foreachC() loops.
 * Contains key, val and entry members.
 */
struct json_object_iter {
	char *key;
	struct json_object *val;
	struct lh_entry *entry;
};
typedef struct json_object_iter json_object_iter;

/**
 * Type of custom user delete functions.  See json_object_set_serializer.
 */
typedef void(json_object_delete_fn)(struct json_object *jso, void *userdata);

/**
 * Type of a custom serialization function.  See json_object_set_serializer.
 */
typedef int(json_object_to_json_string_fn)(struct json_object *jso, struct printbuf *pb, int level, int flags);

typedef enum json_type {
	/* If you change this, be sure to update json_type_to_name() too */
	json_type_null,
	json_type_boolean,
	json_type_double,
	json_type_int,
	json_type_object,
	json_type_array,
	json_type_string
} json_type;
} // namespace json
