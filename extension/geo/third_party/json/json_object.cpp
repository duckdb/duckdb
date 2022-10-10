#include "linkhash.hpp"
#include "printbuf.hpp"

#include <cstring>

namespace json {

const char *json_hex_chars = "0123456789abcdefABCDEF";

#if SIZEOF_LONG_LONG != SIZEOF_INT64_T
#error "The long long type isn't 64-bits"
#endif

#ifndef SSIZE_T_MAX
#if SIZEOF_SSIZE_T == SIZEOF_INT
#define SSIZE_T_MAX INT_MAX
#elif SIZEOF_SSIZE_T == SIZEOF_LONG
#define SSIZE_T_MAX LONG_MAX
#elif SIZEOF_SSIZE_T == SIZEOF_LONG_LONG
#define SSIZE_T_MAX LLONG_MAX
#else
#error Unable to determine size of ssize_t
#endif
#endif

/* Avoid ctype.h and locale overhead */
#define is_plain_digit(c) ((c) >= '0' && (c) <= '9')

static void json_abort(const char *message) {
	if (message != NULL)
		fprintf(stderr, "json-c aborts with error: %s\n", message);
	abort();
}

static json_object_to_json_string_fn json_object_boolean_to_json_string;
static json_object_to_json_string_fn json_object_double_to_json_string_default;
static json_object_to_json_string_fn json_object_array_to_json_string;
static json_object_to_json_string_fn _json_object_userdata_to_json_string;

/*
 * Helper functions to more safely cast to a particular type of json_object
 */
static inline struct json_object_boolean *JC_BOOL(struct json_object *jso) {
	return (json_object_boolean *)(void *)jso;
}

static inline const struct json_object_boolean *JC_BOOL_C(const struct json_object *jso) {
	return (json_object_boolean *)(const void *)jso;
}

static inline struct json_object_double *JC_DOUBLE(struct json_object *jso) {
	return (json_object_double *)(void *)jso;
}

static inline const struct json_object_double *JC_DOUBLE_C(const struct json_object *jso) {
	return (json_object_double *)(const void *)jso;
}

static inline struct json_object_object *JC_OBJECT(struct json_object *jso) {
	return (json_object_object *)(void *)jso;
}

static inline struct json_object_int *JC_INT(struct json_object *jso) {
	return (json_object_int *)(void *)jso;
}

static inline const struct json_object_int *JC_INT_C(const struct json_object *jso) {
	return (json_object_int *)(const void *)jso;
}

static inline const struct json_object_object *JC_OBJECT_C(const struct json_object *jso) {
	return (json_object_object *)(const void *)jso;
}

static inline struct json_object_array *JC_ARRAY(struct json_object *jso) {
	return (json_object_array *)(void *)jso;
}

static inline const struct json_object_array *JC_ARRAY_C(const struct json_object *jso) {
	return (json_object_array *)(const void *)jso;
}

static inline struct json_object_string *JC_STRING(struct json_object *jso) {
	return (json_object_string *)(void *)jso;
}

static inline const struct json_object_string *JC_STRING_C(const struct json_object *jso) {
	return (json_object_string *)(const void *)jso;
}

static inline struct json_object *json_object_new(enum json_type o_type, size_t alloc_size,
                                                  json_object_to_json_string_fn *to_json_string);

/* string escaping */

static int json_escape_str(struct printbuf *pb, const char *str, size_t len, int flags) {
	size_t pos = 0, start_offset = 0;
	unsigned char c;
	while (len--) {
		c = str[pos];
		switch (c) {
		case '\b':
		case '\n':
		case '\r':
		case '\t':
		case '\f':
		case '"':
		case '\\':
		case '/':
			if ((flags & JSON_C_TO_STRING_NOSLASHESCAPE) && c == '/') {
				pos++;
				break;
			}

			if (pos > start_offset)
				printbuf_memappend(pb, str + start_offset, pos - start_offset);

			if (c == '\b')
				printbuf_memappend(pb, "\\b", 2);
			else if (c == '\n')
				printbuf_memappend(pb, "\\n", 2);
			else if (c == '\r')
				printbuf_memappend(pb, "\\r", 2);
			else if (c == '\t')
				printbuf_memappend(pb, "\\t", 2);
			else if (c == '\f')
				printbuf_memappend(pb, "\\f", 2);
			else if (c == '"')
				printbuf_memappend(pb, "\\\"", 2);
			else if (c == '\\')
				printbuf_memappend(pb, "\\\\", 2);
			else if (c == '/')
				printbuf_memappend(pb, "\\/", 2);

			start_offset = ++pos;
			break;
		default:
			if (c < ' ') {
				char sbuf[7];
				if (pos > start_offset)
					printbuf_memappend(pb, str + start_offset, pos - start_offset);
				snprintf(sbuf, sizeof(sbuf), "\\u00%c%c", json_hex_chars[c >> 4], json_hex_chars[c & 0xf]);
				printbuf_memappend_fast(pb, sbuf, (int)sizeof(sbuf) - 1);
				start_offset = ++pos;
			} else
				pos++;
		}
	}
	if (pos > start_offset)
		printbuf_memappend(pb, str + start_offset, pos - start_offset);
	return 0;
}

/* helper for accessing the optimized string data component in json_object
 */
static inline char *get_string_component_mutable(struct json_object *jso) {
	if (JC_STRING_C(jso)->len < 0) {
		/* Due to json_object_set_string(), we might have a pointer */
		return JC_STRING(jso)->c_string.pdata;
	}
	return JC_STRING(jso)->c_string.idata;
}

static inline const char *get_string_component(const struct json_object *jso) {
	return get_string_component_mutable((struct json_object *)(void *)(uintptr_t)(const void *)jso);
}

/* json_object_string */

static int json_object_string_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	std::ptrdiff_t len = JC_STRING(jso)->len;
	printbuf_strappend(pb, "\"");
	json_escape_str(pb, get_string_component(jso), len < 0 ? -(std::ptrdiff_t)len : len, flags);
	printbuf_strappend(pb, "\"");
	return 0;
}

static void json_object_array_entry_free(void *data) {
	json_object_put((struct json_object *)data);
}

static struct json_object *_json_object_new_string(const char *s, const size_t len) {
	size_t objsize;
	struct json_object_string *jso;

	/*
	 * Structures           Actual memory layout
	 * -------------------  --------------------
	 * [json_object_string  [json_object_string
	 *  [json_object]        [json_object]
	 *  ...other fields...   ...other fields...
	 *  c_string]            len
	 *                       bytes
	 *                       of
	 *                       string
	 *                       data
	 *                       \0]
	 */
	if (len > (SSIZE_T_MAX - (sizeof(*jso) - sizeof(jso->c_string)) - 1))
		return NULL;
	objsize = (sizeof(*jso) - sizeof(jso->c_string)) + len + 1;
	if (len < sizeof(void *))
		// We need a minimum size to support json_object_set_string() mutability
		// so we can stuff a pointer into pdata :(
		objsize += sizeof(void *) - len;

	jso = (struct json_object_string *)json_object_new(json_type_string, objsize, &json_object_string_to_json_string);

	if (!jso)
		return NULL;
	jso->len = len;
	memcpy(jso->c_string.idata, s, len);
	// Cast below needed for Clang UB sanitizer
	((char *)jso->c_string.idata)[len] = '\0';
	return &jso->base;
}

static void indent(struct printbuf *pb, int level, int flags) {
	if (flags & JSON_C_TO_STRING_PRETTY) {
		if (flags & JSON_C_TO_STRING_PRETTY_TAB) {
			printbuf_memset(pb, -1, '\t', level);
		} else {
			printbuf_memset(pb, -1, ' ', level * 2);
		}
	}
}

#define JC_CONCAT(a, b)     a##b
#define JC_CONCAT3(a, b, c) a##b##c

#define JSON_OBJECT_NEW(jtype)                                                                                         \
	(struct JC_CONCAT(json_object_, jtype) *)json_object_new(JC_CONCAT(json_type_, jtype),                             \
	                                                         sizeof(struct JC_CONCAT(json_object_, jtype)),            \
	                                                         &JC_CONCAT3(json_object_, jtype, _to_json_string))

static int json_object_object_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	int had_children = 0;
	struct json_object_iter iter;

	printbuf_strappend(pb, "{" /*}*/);
	if (flags & JSON_C_TO_STRING_PRETTY)
		printbuf_strappend(pb, "\n");
	json_object_object_foreachC(jso, iter) {
		if (had_children) {
			printbuf_strappend(pb, ",");
			if (flags & JSON_C_TO_STRING_PRETTY)
				printbuf_strappend(pb, "\n");
		}
		had_children = 1;
		if (flags & JSON_C_TO_STRING_SPACED && !(flags & JSON_C_TO_STRING_PRETTY))
			printbuf_strappend(pb, " ");
		indent(pb, level + 1, flags);
		printbuf_strappend(pb, "\"");
		json_escape_str(pb, iter.key, strlen(iter.key), flags);
		if (flags & JSON_C_TO_STRING_SPACED)
			printbuf_strappend(pb, "\": ");
		else
			printbuf_strappend(pb, "\":");
		if (iter.val == NULL)
			printbuf_strappend(pb, "null");
		else if (iter.val->_to_json_string(iter.val, pb, level + 1, flags) < 0)
			return -1;
	}
	if (flags & JSON_C_TO_STRING_PRETTY) {
		if (had_children)
			printbuf_strappend(pb, "\n");
		indent(pb, level, flags);
	}
	if (flags & JSON_C_TO_STRING_SPACED && !(flags & JSON_C_TO_STRING_PRETTY))
		return printbuf_strappend(pb, /*{*/ " }");
	else
		return printbuf_strappend(pb, /*{*/ "}");
}

/* json_object_int */

static int json_object_int_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	/* room for 19 digits, the sign char, and a null term */
	char sbuf[21];
	if (JC_INT(jso)->cint_type == json_object_int_type_int64)
		snprintf(sbuf, sizeof(sbuf), "%" PRId64, JC_INT(jso)->cint.c_int64);
	else
		snprintf(sbuf, sizeof(sbuf), "%" PRIu64, JC_INT(jso)->cint.c_uint64);
	return printbuf_memappend(pb, sbuf, strlen(sbuf));
}

double json_object_get_double(const struct json_object *jso) {
	double cdouble;
	char *errPtr = NULL;

	if (!jso)
		return 0.0;
	switch (jso->o_type) {
	case json_type_double:
		return JC_DOUBLE_C(jso)->c_double;
	case json_type_int:
		switch (JC_INT_C(jso)->cint_type) {
		case json_object_int_type_int64:
			return JC_INT_C(jso)->cint.c_int64;
		case json_object_int_type_uint64:
			return JC_INT_C(jso)->cint.c_uint64;
		default:
			json_abort("invalid cint_type");
		}
	case json_type_boolean:
		return JC_BOOL_C(jso)->c_boolean;
	case json_type_string:
		errno = 0;
		cdouble = strtod(get_string_component(jso), &errPtr);

		/* if conversion stopped at the first character, return 0.0 */
		if (errPtr == get_string_component(jso)) {
			errno = EINVAL;
			return 0.0;
		}

		/*
		 * Check that the conversion terminated on something sensible
		 *
		 * For example, { "pay" : 123AB } would parse as 123.
		 */
		if (*errPtr != '\0') {
			errno = EINVAL;
			return 0.0;
		}

		/*
		 * If strtod encounters a string which would exceed the
		 * capacity of a double, it returns +/- HUGE_VAL and sets
		 * errno to ERANGE. But +/- HUGE_VAL is also a valid result
		 * from a conversion, so we need to check errno.
		 *
		 * Underflow also sets errno to ERANGE, but it returns 0 in
		 * that case, which is what we will return anyway.
		 *
		 * See CERT guideline ERR30-C
		 */
		if ((HUGE_VAL == cdouble || -HUGE_VAL == cdouble) && (ERANGE == errno))
			cdouble = 0.0;
		return cdouble;
	default:
		errno = EINVAL;
		return 0.0;
	}
}

const char *json_object_get_string(struct json_object *jso) {
	if (!jso)
		return NULL;
	switch (jso->o_type) {
	case json_type_string:
		return get_string_component(jso);
	default:
		return json_object_to_json_string(jso);
	}
}

const char *json_object_to_json_string(struct json_object *jso) {
	return json_object_to_json_string_ext(jso, JSON_C_TO_STRING_SPACED);
}

const char *json_object_to_json_string_ext(struct json_object *jso, int flags) {
	return json_object_to_json_string_length(jso, flags, NULL);
}

void json_object_set_userdata(json_object *jso, void *userdata, json_object_delete_fn *user_delete) {
	// Can't return failure, so abort if we can't perform the operation.
	assert(jso != NULL);

	// First, clean up any previously existing user info
	if (jso->_user_delete)
		jso->_user_delete(jso, jso->_userdata);

	jso->_userdata = userdata;
	jso->_user_delete = user_delete;
}

/* set a custom conversion to string */

void json_object_set_serializer(json_object *jso, json_object_to_json_string_fn *to_string_func, void *userdata,
                                json_object_delete_fn *user_delete) {
	json_object_set_userdata(jso, userdata, user_delete);

	if (to_string_func == NULL) {
		// Reset to the standard serialization function
		switch (jso->o_type) {
		case json_type_null:
			jso->_to_json_string = NULL;
			break;
		case json_type_boolean:
			jso->_to_json_string = &json_object_boolean_to_json_string;
			break;
		case json_type_double:
			jso->_to_json_string = &json_object_double_to_json_string_default;
			break;
		case json_type_int:
			jso->_to_json_string = &json_object_int_to_json_string;
			break;
		case json_type_object:
			jso->_to_json_string = &json_object_object_to_json_string;
			break;
		case json_type_array:
			jso->_to_json_string = &json_object_array_to_json_string;
			break;
		case json_type_string:
			jso->_to_json_string = &json_object_string_to_json_string;
			break;
		}
		return;
	}

	jso->_to_json_string = to_string_func;
}

/* json_object_array */

static int json_object_array_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	int had_children = 0;
	size_t ii;

	printbuf_strappend(pb, "[");
	if (flags & JSON_C_TO_STRING_PRETTY)
		printbuf_strappend(pb, "\n");
	for (ii = 0; ii < json_object_array_length(jso); ii++) {
		struct json_object *val;
		if (had_children) {
			printbuf_strappend(pb, ",");
			if (flags & JSON_C_TO_STRING_PRETTY)
				printbuf_strappend(pb, "\n");
		}
		had_children = 1;
		if (flags & JSON_C_TO_STRING_SPACED && !(flags & JSON_C_TO_STRING_PRETTY))
			printbuf_strappend(pb, " ");
		indent(pb, level + 1, flags);
		val = json_object_array_get_idx(jso, ii);
		if (val == NULL)
			printbuf_strappend(pb, "null");
		else if (val->_to_json_string(val, pb, level + 1, flags) < 0)
			return -1;
	}
	if (flags & JSON_C_TO_STRING_PRETTY) {
		if (had_children)
			printbuf_strappend(pb, "\n");
		indent(pb, level, flags);
	}

	if (flags & JSON_C_TO_STRING_SPACED && !(flags & JSON_C_TO_STRING_PRETTY))
		return printbuf_strappend(pb, " ]");
	return printbuf_strappend(pb, "]");
}

/* extended conversion to string */

const char *json_object_to_json_string_length(struct json_object *jso, int flags, size_t *length) {
	const char *r = NULL;
	size_t s = 0;

	if (!jso) {
		s = 4;
		r = "null";
	} else if ((jso->_pb) || (jso->_pb = printbuf_new())) {
		printbuf_reset(jso->_pb);

		if (jso->_to_json_string(jso, jso->_pb, 0, flags) >= 0) {
			s = (size_t)jso->_pb->bpos;
			r = jso->_pb->buf;
		}
	}

	if (length)
		*length = s;
	return r;
}

static char *global_serialization_float_format = NULL;

static int json_object_double_to_json_string_format(struct json_object *jso, struct printbuf *pb, int level, int flags,
                                                    const char *format) {
	struct json_object_double *jsodbl = JC_DOUBLE(jso);
	char buf[128], *p, *q;
	int size;
	/* Although JSON RFC does not support
	 * NaN or Infinity as numeric values
	 * ECMA 262 section 9.8.1 defines
	 * how to handle these cases as strings
	 */
	if (isnan(jsodbl->c_double)) {
		size = snprintf(buf, sizeof(buf), "NaN");
	} else if (isinf(jsodbl->c_double)) {
		if (jsodbl->c_double > 0)
			size = snprintf(buf, sizeof(buf), "Infinity");
		else
			size = snprintf(buf, sizeof(buf), "-Infinity");
	} else {
		const char *std_format = "%.17g";
		int format_drops_decimals = 0;
		int looks_numeric = 0;

		if (!format) {
#if defined(HAVE___THREAD)
			if (tls_serialization_float_format)
				format = tls_serialization_float_format;
			else
#endif
			    if (global_serialization_float_format)
				format = global_serialization_float_format;
			else
				format = std_format;
		}
		size = snprintf(buf, sizeof(buf), format, jsodbl->c_double);

		if (size < 0)
			return -1;

		p = strchr(buf, ',');
		if (p)
			*p = '.';
		else
			p = strchr(buf, '.');

		if (format == std_format || strstr(format, ".0f") == NULL)
			format_drops_decimals = 1;

		looks_numeric = /* Looks like *some* kind of number */
		    is_plain_digit(buf[0]) || (size > 1 && buf[0] == '-' && is_plain_digit(buf[1]));

		if (size < (int)sizeof(buf) - 2 && looks_numeric && !p && /* Has no decimal point */
		    strchr(buf, 'e') == NULL &&                           /* Not scientific notation */
		    format_drops_decimals) {
			// Ensure it looks like a float, even if snprintf didn't,
			//  unless a custom format is set to omit the decimal.
			strcat(buf, ".0");
			size += 2;
		}
		if (p && (flags & JSON_C_TO_STRING_NOZERO)) {
			/* last useful digit, always keep 1 zero */
			p++;
			for (q = p; *q; q++) {
				if (*q != '0')
					p = q;
			}
			/* drop trailing zeroes */
			if (*p != 0)
				*(++p) = 0;
			size = p - buf;
		}
	}
	// although unlikely, snprintf can fail
	if (size < 0)
		return -1;

	if (size >= (int)sizeof(buf))
		// The standard formats are guaranteed not to overrun the buffer,
		// but if a custom one happens to do so, just silently truncate.
		size = sizeof(buf) - 1;
	printbuf_memappend(pb, buf, size);
	return size;
}

int json_object_double_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	return json_object_double_to_json_string_format(jso, pb, level, flags, (const char *)jso->_userdata);
}

/* json_object_boolean */

static int json_object_boolean_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	if (JC_BOOL(jso)->c_boolean)
		return printbuf_strappend(pb, "true");
	return printbuf_strappend(pb, "false");
}

static void json_object_generic_delete(struct json_object *jso) {
	printbuf_free(jso->_pb);
	free(jso);
}

static void json_object_lh_entry_free(struct lh_entry *ent) {
	if (!lh_entry_k_is_constant(ent))
		free(lh_entry_k(ent));
	json_object_put((struct json_object *)lh_entry_v(ent));
}

static void json_object_object_delete(struct json_object *jso_base) {
	lh_table_free(JC_OBJECT(jso_base)->c_object);
	json_object_generic_delete(jso_base);
}

static void json_object_array_delete(struct json_object *jso) {
	array_list_free(JC_ARRAY(jso)->c_array);
	json_object_generic_delete(jso);
}

static void json_object_string_delete(struct json_object *jso) {
	if (JC_STRING(jso)->len < 0)
		free(JC_STRING(jso)->c_string.pdata);
	json_object_generic_delete(jso);
}

/* reference counting */

struct json_object *json_object_get(struct json_object *jso) {
	if (!jso)
		return jso;

	// Don't overflow the refcounter.
	assert(jso->_ref_count < UINT32_MAX);

	++jso->_ref_count;

	return jso;
}

struct lh_table *json_object_get_object(const struct json_object *jso) {
	if (!jso)
		return nullptr;
	switch (jso->o_type) {
	case json_type_object:
		return JC_OBJECT_C(jso)->c_object;
	default:
		return nullptr;
	}
}

enum json_type json_object_get_type(const struct json_object *jso) {
	if (!jso)
		return json_type_null;
	return jso->o_type;
}

size_t json_object_array_length(const struct json_object *jso) {
	assert(json_object_get_type(jso) == json_type_array);
	return array_list_length(JC_ARRAY_C(jso)->c_array);
}

int json_object_object_add_ex(struct json_object *jso, const char *const key, struct json_object *const val,
                              const unsigned opts) {
	struct json_object *existing_value = NULL;
	struct lh_entry *existing_entry;
	unsigned long hash;

	assert(json_object_get_type(jso) == json_type_object);

	// We lookup the entry and replace the value, rather than just deleting
	// and re-adding it, so the existing key remains valid.
	hash = lh_get_hash(JC_OBJECT(jso)->c_object, (const void *)key);
	existing_entry = (opts & JSON_C_OBJECT_ADD_KEY_IS_NEW)
	                     ? NULL
	                     : lh_table_lookup_entry_w_hash(JC_OBJECT(jso)->c_object, (const void *)key, hash);

	// The caller must avoid creating loops in the object tree, but do a
	// quick check anyway to make sure we're not creating a trivial loop.
	if (jso == val)
		return -1;

	if (!existing_entry) {
		const void *const k = (opts & JSON_C_OBJECT_ADD_CONSTANT_KEY) ? (const void *)key : strdup(key);
		if (k == NULL)
			return -1;
		return lh_table_insert_w_hash(JC_OBJECT(jso)->c_object, k, val, hash, opts);
	}
	existing_value = (json_object *)lh_entry_v(existing_entry);
	if (existing_value)
		json_object_put(existing_value);
	lh_entry_set_val(existing_entry, val);
	return 0;
}

int json_object_object_add(struct json_object *jso, const char *key, struct json_object *val) {
	return json_object_object_add_ex(jso, key, val, 0);
}

int json_object_array_add(struct json_object *jso, struct json_object *val) {
	assert(json_object_get_type(jso) == json_type_array);
	return array_list_add(JC_ARRAY(jso)->c_array, val);
}

struct json_object *json_object_array_get_idx(const struct json_object *jso, size_t idx) {
	assert(json_object_get_type(jso) == json_type_array);
	return (struct json_object *)array_list_get_idx(JC_ARRAY_C(jso)->c_array, idx);
}

int json_object_put(struct json_object *jso) {
	if (!jso)
		return 0;

	/* Avoid invalid free and crash explicitly instead of (silently)
	 * segfaulting.
	 */
	assert(jso->_ref_count > 0);

	if (--jso->_ref_count > 0)
		return 0;

	if (jso->_user_delete)
		jso->_user_delete(jso, jso->_userdata);
	switch (jso->o_type) {
	case json_type_object:
		json_object_object_delete(jso);
		break;
	case json_type_array:
		json_object_array_delete(jso);
		break;
	case json_type_string:
		json_object_string_delete(jso);
		break;
	default:
		json_object_generic_delete(jso);
		break;
	}
	return 1;
}

static inline struct json_object *json_object_new(enum json_type o_type, size_t alloc_size,
                                                  json_object_to_json_string_fn *to_json_string) {
	struct json_object *jso;

	jso = (struct json_object *)malloc(alloc_size);
	if (!jso)
		return NULL;

	jso->o_type = o_type;
	jso->_ref_count = 1;
	jso->_to_json_string = to_json_string;
	jso->_pb = NULL;
	jso->_user_delete = NULL;
	jso->_userdata = NULL;
	// jso->...   // Type-specific fields must be set by caller

	return jso;
}

struct json_object *json_object_new_object(void) {
	struct json_object_object *jso = JSON_OBJECT_NEW(object);
	if (!jso)
		return NULL;
	jso->c_object = lh_kchar_table_new(JSON_OBJECT_DEF_HASH_ENTRIES, &json_object_lh_entry_free);
	if (!jso->c_object) {
		json_object_generic_delete(&jso->base);
		errno = ENOMEM;
		return NULL;
	}
	return &jso->base;
}

static int json_object_double_to_json_string_default(struct json_object *jso, struct printbuf *pb, int level,
                                                     int flags) {
	return json_object_double_to_json_string_format(jso, pb, level, flags, NULL);
}

struct json_object *json_object_new_array(void) {
	return json_object_new_array_ext(ARRAY_LIST_DEFAULT_SIZE);
}

struct json_object *json_object_new_array_ext(int initial_size) {
	struct json_object_array *jso = JSON_OBJECT_NEW(array);
	if (!jso)
		return NULL;
	jso->c_array = array_list_new2(&json_object_array_entry_free, initial_size);
	if (jso->c_array == NULL) {
		free(jso);
		return NULL;
	}
	return &jso->base;
}

struct json_object *json_object_new_boolean(json_bool b) {
	struct json_object_boolean *jso = JSON_OBJECT_NEW(boolean);
	if (!jso)
		return NULL;
	jso->c_boolean = b;
	return &jso->base;
}

int json_object_array_shrink(struct json_object *jso, int empty_slots) {
	if (empty_slots < 0)
		json_abort("json_object_array_shrink called with negative empty_slots");
	return array_list_shrink(JC_ARRAY(jso)->c_array, empty_slots);
}

struct json_object *json_object_new_int64(int64_t i) {
	struct json_object_int *jso = JSON_OBJECT_NEW(int);
	if (!jso)
		return NULL;
	jso->cint.c_int64 = i;
	jso->cint_type = json_object_int_type_int64;
	return &jso->base;
}

struct json_object *json_object_new_uint64(uint64_t i) {
	struct json_object_int *jso = JSON_OBJECT_NEW(int);
	if (!jso)
		return NULL;
	jso->cint.c_uint64 = i;
	jso->cint_type = json_object_int_type_uint64;
	return &jso->base;
}

struct json_object *json_object_new_double(double d) {
	struct json_object_double *jso = JSON_OBJECT_NEW(double);
	if (!jso)
		return NULL;
	jso->base._to_json_string = &json_object_double_to_json_string_default;
	jso->c_double = d;
	return &jso->base;
}

void json_object_free_userdata(struct json_object *jso, void *userdata) {
	free(userdata);
}

struct json_object *json_object_new_double_s(double d, const char *ds) {
	char *new_ds;
	struct json_object *jso = json_object_new_double(d);
	if (!jso)
		return NULL;

	new_ds = strdup(ds);
	if (!new_ds) {
		json_object_generic_delete(jso);
		errno = ENOMEM;
		return NULL;
	}
	json_object_set_serializer(jso, _json_object_userdata_to_json_string, new_ds, json_object_free_userdata);
	return jso;
}

/*
 * A wrapper around json_object_userdata_to_json_string() used only
 * by json_object_new_double_s() just so json_object_set_double() can
 * detect when it needs to reset the serializer to the default.
 */
static int _json_object_userdata_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	return json_object_userdata_to_json_string(jso, pb, level, flags);
}

int json_object_userdata_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags) {
	int userdata_len = strlen((const char *)jso->_userdata);
	printbuf_memappend(pb, (const char *)jso->_userdata, userdata_len);
	return userdata_len;
}

struct json_object *json_object_new_string_len(const char *s, const int len) {
	return _json_object_new_string(s, len);
}

} // namespace json
