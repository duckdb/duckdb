#pragma one

#include "arraylist.hpp"
#include "json_object_private.hpp"

namespace json {

#define JSON_OBJECT_DEF_HASH_ENTRIES 16

/**
 * A flag for the json_object_to_json_string_ext() and
 * json_object_to_file_ext() functions which causes the output to have
 * minimal whitespace inserted to make things slightly more readable.
 */
#define JSON_C_TO_STRING_SPACED (1 << 0)

/**
 * A flag for the json_object_to_json_string_ext() and
 * json_object_to_file_ext() functions which causes
 * the output to be formatted.
 *
 * See the "Two Space Tab" option at http://jsonformatter.curiousconcept.com/
 * for an example of the format.
 */
#define JSON_C_TO_STRING_PRETTY (1 << 1)
/**
 * A flag for the json_object_to_json_string_ext() and
 * json_object_to_file_ext() functions which causes
 * the output to be formatted.
 *
 * Instead of a "Two Space Tab" this gives a single tab character.
 */
#define JSON_C_TO_STRING_PRETTY_TAB (1 << 3)
/**
 * A flag to drop trailing zero for float values
 */
#define JSON_C_TO_STRING_NOZERO (1 << 2)

/**
 * Don't escape forward slashes.
 */
#define JSON_C_TO_STRING_NOSLASHESCAPE (1 << 4)

/**
 * A flag for the json_object_object_add_ex function which
 * flags the key as being constant memory. This means that
 * the key will NOT be copied via strdup(), resulting in a
 * potentially huge performance win (malloc, strdup and
 * free are usually performance hogs). It is acceptable to
 * use this flag for keys in non-constant memory blocks if
 * the caller ensure that the memory holding the key lives
 * longer than the corresponding json object. However, this
 * is somewhat dangerous and should only be done if really
 * justified.
 * The general use-case for this flag is cases where the
 * key is given as a real constant value in the function
 * call, e.g. as in
 *   json_object_object_add_ex(obj, "ip", json,
 *       JSON_C_OBJECT_ADD_CONSTANT_KEY);
 */
#define JSON_C_OBJECT_ADD_CONSTANT_KEY (1 << 2)

/**
 * This flag is an alias to JSON_C_OBJECT_ADD_CONSTANT_KEY.
 * Historically, this flag was used first and the new name
 * JSON_C_OBJECT_ADD_CONSTANT_KEY was introduced for version
 * 0.16.00 in order to have regular naming.
 * Use of this flag is now legacy.
 */
#define JSON_C_OBJECT_KEY_IS_CONSTANT JSON_C_OBJECT_ADD_CONSTANT_KEY

/**
 * A flag for the json_object_object_add_ex function which
 * causes the value to be added without a check if it already exists.
 * Note: it is the responsibility of the caller to ensure that no
 * key is added multiple times. If this is done, results are
 * unpredictable. While this option is somewhat dangerous, it
 * permits potentially large performance savings in code that
 * knows for sure the key values are unique (e.g. because the
 * code adds a well-known set of constant key values).
 */
#define JSON_C_OBJECT_ADD_KEY_IS_NEW (1 << 1)

/**
 * A flag for the json_object_object_add_ex function which
 * flags the key as being constant memory. This means that
 * the key will NOT be copied via strdup(), resulting in a
 * potentially huge performance win (malloc, strdup and
 * free are usually performance hogs). It is acceptable to
 * use this flag for keys in non-constant memory blocks if
 * the caller ensure that the memory holding the key lives
 * longer than the corresponding json object. However, this
 * is somewhat dangerous and should only be done if really
 * justified.
 * The general use-case for this flag is cases where the
 * key is given as a real constant value in the function
 * call, e.g. as in
 *   json_object_object_add_ex(obj, "ip", json,
 *       JSON_C_OBJECT_ADD_CONSTANT_KEY);
 */
#define JSON_C_OBJECT_ADD_CONSTANT_KEY (1 << 2)

/**
 * Simply call free on the userdata pointer.
 * Can be used with json_object_set_serializer().
 *
 * @param jso unused
 * @param userdata the pointer that is passed to free().
 */
extern json_object_delete_fn json_object_free_userdata;

/**
 * Copy the jso->_userdata string over to pb as-is.
 * Can be used with json_object_set_serializer().
 *
 * @param jso The object whose _userdata is used.
 * @param pb The destination buffer.
 * @param level Ignored.
 * @param flags Ignored.
 */
extern json_object_to_json_string_fn json_object_userdata_to_json_string;

/** Iterate through all keys and values of an object (ANSI C Safe)
 * @param obj the json_object instance
 * @param iter the object iterator, use type json_object_iter
 */
#define json_object_object_foreachC(obj, iter)                                                                         \
	for (iter.entry = lh_table_head(json_object_get_object(obj));                                                      \
	     (iter.entry ? (iter.key = (char *)lh_entry_k(iter.entry),                                                     \
	                   iter.val = (struct json_object *)lh_entry_v(iter.entry), iter.entry)                            \
	                 : 0);                                                                                             \
	     iter.entry = lh_entry_next(iter.entry))

/**
 * Get the type of the json_object.  See also json_type_to_name() to turn this
 * into a string suitable, for instance, for logging.
 *
 * @param obj the json_object instance
 * @returns type being one of:
     json_type_null (i.e. obj == NULL),
     json_type_boolean,
     json_type_double,
     json_type_int,
     json_type_object,
     json_type_array,
     json_type_string
 */
extern enum json_type json_object_get_type(const struct json_object *obj);

/** Get the hashtable of a json_object of type json_type_object
 * @param obj the json_object instance
 * @returns a linkhash
 */
extern struct lh_table *json_object_get_object(const struct json_object *obj);

/** Get the length of a json_object of type json_type_array
 * @param obj the json_object instance
 * @returns an int
 */
extern size_t json_object_array_length(const struct json_object *obj);

/** Add an element to the end of a json_object of type json_type_array
 *
 * The reference count will *not* be incremented. This is to make adding
 * fields to objects in code more compact. If you want to retain a reference
 * to an added object you must wrap the passed object with json_object_get
 *
 * @param obj the json_object instance
 * @param val the json_object to be added
 */
extern int json_object_array_add(struct json_object *obj, struct json_object *val);

/** Get the element at specified index of array `obj` (which must be a json_object of type json_type_array)
 *
 * *No* reference counts will be changed, and ownership of the returned
 * object remains with `obj`.  See json_object_object_get() for additional
 * implications of this behavior.
 *
 * Calling this with anything other than a json_type_array will trigger
 * an assert.
 *
 * @param obj the json_object instance
 * @param idx the index to get the element at
 * @returns the json_object at the specified index (or NULL)
 */
extern struct json_object *json_object_array_get_idx(const struct json_object *obj, size_t idx);

/**
 * Increment the reference count of json_object, thereby taking ownership of it.
 *
 * Cases where you might need to increase the refcount include:
 * - Using an object field or array index (retrieved through
 *    `json_object_object_get()` or `json_object_array_get_idx()`)
 *    beyond the lifetime of the parent object.
 * - Detaching an object field or array index from its parent object
 *    (using `json_object_object_del()` or `json_object_array_del_idx()`)
 * - Sharing a json_object with multiple (not necesarily parallel) threads
 *    of execution that all expect to free it (with `json_object_put()`) when
 *    they're done.
 *
 * @param obj the json_object instance
 * @see json_object_put()
 * @see json_object_object_get()
 * @see json_object_array_get_idx()
 */
extern struct json_object *json_object_get(struct json_object *obj);

/** Get the double floating point value of a json_object
 *
 * The type is coerced to a double if the passed object is not a double.
 * integer objects will return their double conversion. Strings will be
 * parsed as a double. If no conversion exists then 0.0 is returned and
 * errno is set to EINVAL. null is equivalent to 0 (no error values set)
 *
 * If the value is too big to fit in a double, then the value is set to
 * the closest infinity with errno set to ERANGE. If strings cannot be
 * converted to their double value, then EINVAL is set & NaN is returned.
 *
 * Arrays of length 0 are interpreted as 0 (with no error flags set).
 * Arrays of length 1 are effectively cast to the equivalent object and
 * converted using the above rules.  All other arrays set the error to
 * EINVAL & return NaN.
 *
 * NOTE: Set errno to 0 directly before a call to this function to
 * determine whether or not conversion was successful (it does not clear
 * the value for you).
 *
 * @param obj the json_object instance
 * @returns a double floating point number
 */
extern double json_object_get_double(const struct json_object *obj);

/** Get the string value of a json_object
 *
 * If the passed object is of type json_type_null (i.e. obj == NULL),
 * NULL is returned.
 *
 * If the passed object of type json_type_string, the string contents
 * are returned.
 *
 * Otherwise the JSON representation of the object is returned.
 *
 * The returned string memory is managed by the json_object and will
 * be freed when the reference count of the json_object drops to zero.
 *
 * @param obj the json_object instance
 * @returns a string or NULL
 */
extern const char *json_object_get_string(struct json_object *obj);

/**
 * Set an opaque userdata value for an object
 *
 * The userdata can be retrieved using json_object_get_userdata().
 *
 * If custom userdata is already set on this object, any existing user_delete
 * function is called before the new one is set.
 *
 * The user_delete parameter is optional and may be passed as NULL, even if
 * the userdata parameter is non-NULL.  It will be called just before the
 * json_object is deleted, after it's reference count goes to zero
 * (see json_object_put()).
 * If this is not provided, it is up to the caller to free the userdata at
 * an appropriate time. (i.e. after the json_object is deleted)
 *
 * Note: Objects created by parsing strings may have custom serializers set
 * which expect the userdata to contain specific data (due to use of
 * json_object_new_double_s()). In this case, json_object_set_serialiser() with
 * NULL as to_string_func should be used instead to set the userdata and reset
 * the serializer to its default value.
 *
 * @param jso the object to set the userdata for
 * @param userdata an optional opaque cookie
 * @param user_delete an optional function from freeing userdata
 */
extern void json_object_set_userdata(json_object *jso, void *userdata, json_object_delete_fn *user_delete);

/**
 * Set a custom serialization function to be used when this particular object
 * is converted to a string by json_object_to_json_string.
 *
 * If custom userdata is already set on this object, any existing user_delete
 * function is called before the new one is set.
 *
 * If to_string_func is NULL the default behaviour is reset (but the userdata
 * and user_delete fields are still set).
 *
 * The userdata parameter is optional and may be passed as NULL. It can be used
 * to provide additional data for to_string_func to use. This parameter may
 * be NULL even if user_delete is non-NULL.
 *
 * The user_delete parameter is optional and may be passed as NULL, even if
 * the userdata parameter is non-NULL.  It will be called just before the
 * json_object is deleted, after it's reference count goes to zero
 * (see json_object_put()).
 * If this is not provided, it is up to the caller to free the userdata at
 * an appropriate time. (i.e. after the json_object is deleted)
 *
 * Note that the userdata is the same as set by json_object_set_userdata(), so
 * care must be taken not to overwrite the value when both a custom serializer
 * and json_object_set_userdata() are used.
 *
 * @param jso the object to customize
 * @param to_string_func the custom serialization function
 * @param userdata an optional opaque cookie
 * @param user_delete an optional function from freeing userdata
 */
extern void json_object_set_serializer(json_object *jso, json_object_to_json_string_fn *to_string_func, void *userdata,
                                       json_object_delete_fn *user_delete);

/** Stringify object to json format.
 * Equivalent to json_object_to_json_string_ext(obj, JSON_C_TO_STRING_SPACED)
 * The pointer you get is an internal of your json object. You don't
 * have to free it, later use of json_object_put() should be sufficient.
 * If you can not ensure there's no concurrent access to *obj use
 * strdup().
 * @param obj the json_object instance
 * @returns a string in JSON format
 */
extern const char *json_object_to_json_string(struct json_object *obj);

/** Stringify object to json format
 * @see json_object_to_json_string() for details on how to free string.
 * @param obj the json_object instance
 * @param flags formatting options, see JSON_C_TO_STRING_PRETTY and other constants
 * @returns a string in JSON format
 */
extern const char *json_object_to_json_string_ext(struct json_object *obj, int flags);

/** Stringify object to json format
 * @see json_object_to_json_string() for details on how to free string.
 * @param obj the json_object instance
 * @param flags formatting options, see JSON_C_TO_STRING_PRETTY and other constants
 * @param length a pointer where, if not NULL, the length (without null) is stored
 * @returns a string in JSON format and the length if not NULL
 */
extern const char *json_object_to_json_string_length(struct json_object *obj, int flags, size_t *length);

/** Serialize a json_object of type json_type_double to a string.
 *
 * This function isn't meant to be called directly. Instead, you can set a
 * custom format string for the serialization of this double using the
 * following call (where "%.17g" actually is the default):
 *
 * @code
 *   jso = json_object_new_double(d);
 *   json_object_set_serializer(jso, json_object_double_to_json_string,
 *       "%.17g", NULL);
 * @endcode
 *
 * @see printf(3) man page for format strings
 *
 * @param jso The json_type_double object that is serialized.
 * @param pb The destination buffer.
 * @param level Ignored.
 * @param flags Ignored.
 */
extern int json_object_double_to_json_string(struct json_object *jso, struct printbuf *pb, int level, int flags);

/** Add an object field to a json_object of type json_type_object
 *
 * The reference count of `val` will *not* be incremented, in effect
 * transferring ownership that object to `obj`, and thus `val` will be
 * freed when `obj` is.  (i.e. through `json_object_put(obj)`)
 *
 * If you want to retain a reference to the added object, independent
 * of the lifetime of obj, you must increment the refcount with
 * `json_object_get(val)` (and later release it with json_object_put()).
 *
 * Since ownership transfers to `obj`, you must make sure
 * that you do in fact have ownership over `val`.  For instance,
 * json_object_new_object() will give you ownership until you transfer it,
 * whereas json_object_object_get() does not.
 *
 * Any previous object stored under `key` in `obj` will have its refcount
 * decremented, and be freed normally if that drops to zero.
 *
 * @param obj the json_object instance
 * @param key the object field name (a private copy will be duplicated)
 * @param val a json_object or NULL member to associate with the given field
 *
 * @return On success, <code>0</code> is returned.
 * 	On error, a negative value is returned.
 */
extern int json_object_object_add(struct json_object *obj, const char *key, struct json_object *val);

/** Add an object field to a json_object of type json_type_object
 *
 * The semantics are identical to json_object_object_add, except that an
 * additional flag fields gives you more control over some detail aspects
 * of processing. See the description of JSON_C_OBJECT_ADD_* flags for more
 * details.
 *
 * @param obj the json_object instance
 * @param key the object field name (a private copy will be duplicated)
 * @param val a json_object or NULL member to associate with the given field
 * @param opts process-modifying options. To specify multiple options, use
 *             (OPT1|OPT2)
 */
extern int json_object_object_add_ex(struct json_object *obj, const char *const key, struct json_object *const val,
                                     const unsigned opts);

/**
 * Decrement the reference count of json_object and free if it reaches zero.
 *
 * You must have ownership of obj prior to doing this or you will cause an
 * imbalance in the reference count, leading to a classic use-after-free bug.
 * In particular, you normally do not need to call `json_object_put()` on the
 * json_object returned by `json_object_object_get()` or `json_object_array_get_idx()`.
 *
 * Just like after calling `free()` on a block of memory, you must not use
 * `obj` after calling `json_object_put()` on it or any object that it
 * is a member of (unless you know you've called `json_object_get(obj)` to
 * explicitly increment the refcount).
 *
 * NULL may be passed, which which case this is a no-op.
 *
 * @param obj the json_object instance
 * @returns 1 if the object was freed.
 * @see json_object_get()
 */
extern int json_object_put(struct json_object *obj);

/** Create a new empty object with a reference count of 1.  The caller of
 * this object initially has sole ownership.  Remember, when using
 * json_object_object_add or json_object_array_put_idx, ownership will
 * transfer to the object/array.  Call json_object_get if you want to maintain
 * shared ownership or also add this object as a child of multiple objects or
 * arrays.  Any ownerships you acquired but did not transfer must be released
 * through json_object_put.
 *
 * @returns a json_object of type json_type_object
 */
extern struct json_object *json_object_new_object(void);

/** Create a new empty json_object of type json_type_array
 * with 32 slots allocated.
 * If you know the array size you'll need ahead of time, use
 * json_object_new_array_ext() instead.
 * @see json_object_new_array_ext()
 * @see json_object_array_shrink()
 * @returns a json_object of type json_type_array
 */
extern struct json_object *json_object_new_array(void);

/** Create a new empty json_object of type json_type_array
 * with the desired number of slots allocated.
 * @see json_object_array_shrink()
 * @param initial_size the number of slots to allocate
 * @returns a json_object of type json_type_array
 */
extern struct json_object *json_object_new_array_ext(int initial_size);

/** Create a new empty json_object of type json_type_boolean
 * @param b a json_bool 1 or 0
 * @returns a json_object of type json_type_boolean
 */
extern struct json_object *json_object_new_boolean(json_bool b);

/**
 * Shrink the internal memory allocation of the array to just
 * enough to fit the number of elements in it, plus empty_slots.
 *
 * @param jso the json_object instance, must be json_type_array
 * @param empty_slots the number of empty slots to leave allocated
 */
extern int json_object_array_shrink(struct json_object *jso, int empty_slots);

/** Create a new empty json_object of type json_type_int
 * @param i the integer
 * @returns a json_object of type json_type_int
 */
extern struct json_object *json_object_new_int64(int64_t i);

/** Create a new empty json_object of type json_type_uint
 * @param i the integer
 * @returns a json_object of type json_type_uint
 */
extern struct json_object *json_object_new_uint64(uint64_t i);

/** Create a new empty json_object of type json_type_double
 *
 * @see json_object_double_to_json_string() for how to set a custom format string.
 *
 * @param d the double
 * @returns a json_object of type json_type_double
 */
extern struct json_object *json_object_new_double(double d);

/**
 * Create a new json_object of type json_type_double, using
 * the exact serialized representation of the value.
 *
 * This allows for numbers that would otherwise get displayed
 * inefficiently (e.g. 12.3 => "12.300000000000001") to be
 * serialized with the more convenient form.
 *
 * Notes:
 *
 * This is used by json_tokener_parse_ex() to allow for
 * an exact re-serialization of a parsed object.
 *
 * The userdata field is used to store the string representation, so it
 * can't be used for other data if this function is used.
 *
 * A roughly equivalent sequence of calls, with the difference being that
 *  the serialization function won't be reset by json_object_set_double(), is:
 * @code
 *   jso = json_object_new_double(d);
 *   json_object_set_serializer(jso, json_object_userdata_to_json_string,
 *       strdup(ds), json_object_free_userdata);
 * @endcode
 *
 * @param d the numeric value of the double.
 * @param ds the string representation of the double.  This will be copied.
 */
extern struct json_object *json_object_new_double_s(double d, const char *ds);

/** Create a new empty json_object of type json_type_string and allocate
 * len characters for the new string.
 *
 * A copy of the string is made and the memory is managed by the json_object
 *
 * @param s the string
 * @param len max length of the new string
 * @returns a json_object of type json_type_string
 * @see json_object_new_string()
 */
extern struct json_object *json_object_new_string_len(const char *s, const int len);

} // namespace json
