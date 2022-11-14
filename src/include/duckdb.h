//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

// duplicate of duckdb/main/winapi.hpp
#ifndef DUCKDB_API
#ifdef _WIN32
#if defined(DUCKDB_BUILD_LIBRARY) && !defined(DUCKDB_BUILD_LOADABLE_EXTENSION)
#define DUCKDB_API __declspec(dllexport)
#else
#define DUCKDB_API __declspec(dllimport)
#endif
#else
#define DUCKDB_API
#endif
#endif

// duplicate of duckdb/main/winapi.hpp
#ifndef DUCKDB_EXTENSION_API
#ifdef _WIN32
#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_EXTENSION_API __declspec(dllexport)
#else
#define DUCKDB_EXTENSION_API
#endif
#else
#define DUCKDB_EXTENSION_API __attribute__((visibility("default")))
#endif
#endif

// duplicate of duckdb/common/constants.hpp
#ifndef DUCKDB_API_0_3_1
#define DUCKDB_API_0_3_1 1
#endif
#ifndef DUCKDB_API_0_3_2
#define DUCKDB_API_0_3_2 2
#endif
#ifndef DUCKDB_API_LATEST
#define DUCKDB_API_LATEST DUCKDB_API_0_3_2
#endif

#ifndef DUCKDB_API_VERSION
#define DUCKDB_API_VERSION DUCKDB_API_LATEST
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

//===--------------------------------------------------------------------===//
// Type Information
//===--------------------------------------------------------------------===//
typedef uint64_t idx_t;

typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN,
	// int8_t
	DUCKDB_TYPE_TINYINT,
	// int16_t
	DUCKDB_TYPE_SMALLINT,
	// int32_t
	DUCKDB_TYPE_INTEGER,
	// int64_t
	DUCKDB_TYPE_BIGINT,
	// uint8_t
	DUCKDB_TYPE_UTINYINT,
	// uint16_t
	DUCKDB_TYPE_USMALLINT,
	// uint32_t
	DUCKDB_TYPE_UINTEGER,
	// uint64_t
	DUCKDB_TYPE_UBIGINT,
	// float
	DUCKDB_TYPE_FLOAT,
	// double
	DUCKDB_TYPE_DOUBLE,
	// duckdb_timestamp, in microseconds
	DUCKDB_TYPE_TIMESTAMP,
	// duckdb_date
	DUCKDB_TYPE_DATE,
	// duckdb_time
	DUCKDB_TYPE_TIME,
	// duckdb_interval
	DUCKDB_TYPE_INTERVAL,
	// duckdb_hugeint
	DUCKDB_TYPE_HUGEINT,
	// const char*
	DUCKDB_TYPE_VARCHAR,
	// duckdb_blob
	DUCKDB_TYPE_BLOB,
	// decimal
	DUCKDB_TYPE_DECIMAL,
	// duckdb_timestamp, in seconds
	DUCKDB_TYPE_TIMESTAMP_S,
	// duckdb_timestamp, in milliseconds
	DUCKDB_TYPE_TIMESTAMP_MS,
	// duckdb_timestamp, in nanoseconds
	DUCKDB_TYPE_TIMESTAMP_NS,
	// enum type, only useful as logical type
	DUCKDB_TYPE_ENUM,
	// list type, only useful as logical type
	DUCKDB_TYPE_LIST,
	// struct type, only useful as logical type
	DUCKDB_TYPE_STRUCT,
	// map type, only useful as logical type
	DUCKDB_TYPE_MAP,
	// duckdb_hugeint
	DUCKDB_TYPE_UUID,
	// const char*
	DUCKDB_TYPE_JSON,
	// union type, only useful as logical type
	DUCKDB_TYPE_UNION,
} duckdb_type;

//! Days are stored as days since 1970-01-01
//! Use the duckdb_from_date/duckdb_to_date function to extract individual information
typedef struct {
	int32_t days;
} duckdb_date;

typedef struct {
	int32_t year;
	int8_t month;
	int8_t day;
} duckdb_date_struct;

//! Time is stored as microseconds since 00:00:00
//! Use the duckdb_from_time/duckdb_to_time function to extract individual information
typedef struct {
	int64_t micros;
} duckdb_time;

typedef struct {
	int8_t hour;
	int8_t min;
	int8_t sec;
	int32_t micros;
} duckdb_time_struct;

//! Timestamps are stored as microseconds since 1970-01-01
//! Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information
typedef struct {
	int64_t micros;
} duckdb_timestamp;

typedef struct {
	duckdb_date_struct date;
	duckdb_time_struct time;
} duckdb_timestamp_struct;

typedef struct {
	int32_t months;
	int32_t days;
	int64_t micros;
} duckdb_interval;

//! Hugeints are composed in a (lower, upper) component
//! The value of the hugeint is upper * 2^64 + lower
//! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
typedef struct {
	uint64_t lower;
	int64_t upper;
} duckdb_hugeint;

typedef struct {
	uint8_t width;
	uint8_t scale;

	duckdb_hugeint value;
} duckdb_decimal;

typedef struct {
	char *data;
	idx_t size;
} duckdb_string;

typedef struct {
	void *data;
	idx_t size;
} duckdb_blob;

typedef struct {
#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
	void *data;
	bool *nullmask;
	duckdb_type type;
	char *name;
#else
	// deprecated, use duckdb_column_data
	void *__deprecated_data;
	// deprecated, use duckdb_nullmask_data
	bool *__deprecated_nullmask;
	// deprecated, use duckdb_column_type
	duckdb_type __deprecated_type;
	// deprecated, use duckdb_column_name
	char *__deprecated_name;
#endif
	void *internal_data;
} duckdb_column;

typedef struct {
#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
	idx_t column_count;
	idx_t row_count;
	idx_t rows_changed;
	duckdb_column *columns;
	char *error_message;
#else
	// deprecated, use duckdb_column_count
	idx_t __deprecated_column_count;
	// deprecated, use duckdb_row_count
	idx_t __deprecated_row_count;
	// deprecated, use duckdb_rows_changed
	idx_t __deprecated_rows_changed;
	// deprecated, use duckdb_column_ family of functions
	duckdb_column *__deprecated_columns;
	// deprecated, use duckdb_result_error
	char *__deprecated_error_message;
#endif
	void *internal_data;
} duckdb_result;

typedef void *duckdb_database;
typedef void *duckdb_connection;
typedef void *duckdb_prepared_statement;
typedef void *duckdb_pending_result;
typedef void *duckdb_appender;
typedef void *duckdb_arrow;
typedef void *duckdb_config;
typedef void *duckdb_arrow_schema;
typedef void *duckdb_arrow_array;
typedef void *duckdb_logical_type;
typedef void *duckdb_data_chunk;
typedef void *duckdb_vector;
typedef void *duckdb_value;

typedef enum { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;
typedef enum {
	DUCKDB_PENDING_RESULT_READY = 0,
	DUCKDB_PENDING_RESULT_NOT_READY = 1,
	DUCKDB_PENDING_ERROR = 2
} duckdb_pending_state;

//===--------------------------------------------------------------------===//
// Open/Connect
//===--------------------------------------------------------------------===//

/*!
Creates a new database or opens an existing database file stored at the the given path.
If no path is given a new in-memory database is created instead.

* path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
* out_database: The result database object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);

/*!
Extended version of duckdb_open. Creates a new database or opens an existing database file stored at the the given path.

* path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
* out_database: The result database object.
* config: (Optional) configuration used to start up the database system.
* out_error: If set and the function returns DuckDBError, this will contain the reason why the start-up failed.
Note that the error must be freed using `duckdb_free`.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_open_ext(const char *path, duckdb_database *out_database, duckdb_config config,
                                        char **out_error);

/*!
Closes the specified database and de-allocates all memory allocated for that database.
This should be called after you are done with any database allocated through `duckdb_open`.
Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
Still it is recommended to always correctly close a database object after you are done with it.

* database: The database object to shut down.
*/
DUCKDB_API void duckdb_close(duckdb_database *database);

/*!
Opens a connection to a database. Connections are required to query the database, and store transactional state
associated with the connection.

* database: The database file to connect to.
* out_connection: The result connection object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);

/*!
Closes the specified connection and de-allocates all memory allocated for that connection.

* connection: The connection to close.
*/
DUCKDB_API void duckdb_disconnect(duckdb_connection *connection);

/*!
Returns the version of the linked DuckDB, with a version postfix for dev versions

Usually used for developing C extensions that must return this for a compatibility check.
*/
DUCKDB_API const char *duckdb_library_version();

//===--------------------------------------------------------------------===//
// Configuration
//===--------------------------------------------------------------------===//
/*!
Initializes an empty configuration object that can be used to provide start-up options for the DuckDB instance
through `duckdb_open_ext`.

This will always succeed unless there is a malloc failure.

* out_config: The result configuration object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_create_config(duckdb_config *out_config);

/*!
This returns the total amount of configuration options available for usage with `duckdb_get_config_flag`.

This should not be called in a loop as it internally loops over all the options.

* returns: The amount of config options available.
*/
DUCKDB_API size_t duckdb_config_count();

/*!
Obtains a human-readable name and description of a specific configuration option. This can be used to e.g.
display configuration options. This will succeed unless `index` is out of range (i.e. `>= duckdb_config_count`).

The result name or description MUST NOT be freed.

* index: The index of the configuration option (between 0 and `duckdb_config_count`)
* out_name: A name of the configuration flag.
* out_description: A description of the configuration flag.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_get_config_flag(size_t index, const char **out_name, const char **out_description);

/*!
Sets the specified option for the specified configuration. The configuration option is indicated by name.
To obtain a list of config options, see `duckdb_get_config_flag`.

In the source code, configuration options are defined in `config.cpp`.

This can fail if either the name is invalid, or if the value provided for the option is invalid.

* duckdb_config: The configuration object to set the option on.
* name: The name of the configuration flag to set.
* option: The value to set the configuration flag to.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_set_config(duckdb_config config, const char *name, const char *option);

/*!
Destroys the specified configuration option and de-allocates all memory allocated for the object.

* config: The configuration object to destroy.
*/
DUCKDB_API void duckdb_destroy_config(duckdb_config *config);

//===--------------------------------------------------------------------===//
// Query Execution
//===--------------------------------------------------------------------===//
/*!
Executes a SQL query within a connection and stores the full (materialized) result in the out_result pointer.
If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
`duckdb_result_error`.

Note that after running `duckdb_query`, `duckdb_destroy_result` must be called on the result object even if the
query fails, otherwise the error stored within the result will not be freed correctly.

* connection: The connection to perform the query in.
* query: The SQL query to run.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);

/*!
Closes the result and de-allocates all memory allocated for that connection.

* result: The result to destroy.
*/
DUCKDB_API void duckdb_destroy_result(duckdb_result *result);

/*!
Returns the column name of the specified column. The result should not need be freed; the column names will
automatically be destroyed when the result is destroyed.

Returns `NULL` if the column is out of range.

* result: The result object to fetch the column name from.
* col: The column index.
* returns: The column name of the specified column.
*/
DUCKDB_API const char *duckdb_column_name(duckdb_result *result, idx_t col);

/*!
Returns the column type of the specified column.

Returns `DUCKDB_TYPE_INVALID` if the column is out of range.

* result: The result object to fetch the column type from.
* col: The column index.
* returns: The column type of the specified column.
*/
DUCKDB_API duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);

/*!
Returns the logical column type of the specified column.

The return type of this call should be destroyed with `duckdb_destroy_logical_type`.

Returns `NULL` if the column is out of range.

* result: The result object to fetch the column type from.
* col: The column index.
* returns: The logical column type of the specified column.
*/
DUCKDB_API duckdb_logical_type duckdb_column_logical_type(duckdb_result *result, idx_t col);

/*!
Returns the number of columns present in a the result object.

* result: The result object.
* returns: The number of columns present in the result object.
*/
DUCKDB_API idx_t duckdb_column_count(duckdb_result *result);

/*!
Returns the number of rows present in a the result object.

* result: The result object.
* returns: The number of rows present in the result object.
*/
DUCKDB_API idx_t duckdb_row_count(duckdb_result *result);

/*!
Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
queries. For other queries the rows_changed will be 0.

* result: The result object.
* returns: The number of rows changed.
*/
DUCKDB_API idx_t duckdb_rows_changed(duckdb_result *result);

/*!
**DEPRECATED**: Prefer using `duckdb_result_get_chunk` instead.

Returns the data of a specific column of a result in columnar format.

The function returns a dense array which contains the result data. The exact type stored in the array depends on the
corresponding duckdb_type (as provided by `duckdb_column_type`). For the exact type by which the data should be
accessed, see the comments in [the types section](types) or the `DUCKDB_TYPE` enum.

For example, for a column of type `DUCKDB_TYPE_INTEGER`, rows can be accessed in the following manner:
```c
int32_t *data = (int32_t *) duckdb_column_data(&result, 0);
printf("Data for row %d: %d\n", row, data[row]);
```

* result: The result object to fetch the column data from.
* col: The column index.
* returns: The column data of the specified column.
*/
DUCKDB_API void *duckdb_column_data(duckdb_result *result, idx_t col);

/*!
**DEPRECATED**: Prefer using `duckdb_result_get_chunk` instead.

Returns the nullmask of a specific column of a result in columnar format. The nullmask indicates for every row
whether or not the corresponding row is `NULL`. If a row is `NULL`, the values present in the array provided
by `duckdb_column_data` are undefined.

```c
int32_t *data = (int32_t *) duckdb_column_data(&result, 0);
bool *nullmask = duckdb_nullmask_data(&result, 0);
if (nullmask[row]) {
    printf("Data for row %d: NULL\n", row);
} else {
    printf("Data for row %d: %d\n", row, data[row]);
}
```

* result: The result object to fetch the nullmask from.
* col: The column index.
* returns: The nullmask of the specified column.
*/
DUCKDB_API bool *duckdb_nullmask_data(duckdb_result *result, idx_t col);

/*!
Returns the error message contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.

The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_result` is called.

* result: The result object to fetch the error from.
* returns: The error of the result.
*/
DUCKDB_API const char *duckdb_result_error(duckdb_result *result);

//===--------------------------------------------------------------------===//
// Result Functions
//===--------------------------------------------------------------------===//

/*!
Fetches a data chunk from the duckdb_result. This function should be called repeatedly until the result is exhausted.

This function supersedes all `duckdb_value` functions, as well as the `duckdb_column_data` and `duckdb_nullmask_data`
functions. It results in significantly better performance, and should be preferred in newer code-bases.

If this function is used, none of the other result functions can be used and vice versa (i.e. this function cannot be
mixed with the legacy result functions).

Use `duckdb_result_chunk_count` to figure out how many chunks there are in the result.

* result: The result object to fetch the data chunk from.
* chunk_index: The chunk index to fetch from.
* returns: The resulting data chunk. Returns `NULL` if the chunk index is out of bounds.
*/
DUCKDB_API duckdb_data_chunk duckdb_result_get_chunk(duckdb_result result, idx_t chunk_index);

/*!
Returns the number of data chunks present in the result.

* result: The result object
* returns: The resulting data chunk. Returns `NULL` if the chunk index is out of bounds.
*/
DUCKDB_API idx_t duckdb_result_chunk_count(duckdb_result result);

// Safe fetch functions
// These functions will perform conversions if necessary.
// On failure (e.g. if conversion cannot be performed or if the value is NULL) a default value is returned.
// Note that these functions are slow since they perform bounds checking and conversion
// For fast access of values prefer using `duckdb_result_get_chunk`

/*!
 * returns: The boolean value at the specified location, or false if the value cannot be converted.
 */
DUCKDB_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The int8_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The int16_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The int32_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The int64_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_hugeint value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_decimal value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The uint8_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The uint16_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The uint32_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The uint64_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The float value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The double value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_date value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_time value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_timestamp value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: The duckdb_interval value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);

/*!
* DEPRECATED: use duckdb_value_string instead. This function does not work correctly if the string contains null bytes.
* returns: The text value at the specified location as a null-terminated string, or nullptr if the value cannot be
converted. The result must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);

/*!s
* returns: The string value at the specified location.
The result must be freed with `duckdb_free`.
*/
DUCKDB_API duckdb_string duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);

/*!
* DEPRECATED: use duckdb_value_string_internal instead. This function does not work correctly if the string contains
null bytes.
* returns: The char* value at the specified location. ONLY works on VARCHAR columns and does not auto-cast.
If the column is NOT a VARCHAR column this function will return NULL.

The result must NOT be freed.
*/
DUCKDB_API char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row);

/*!
* DEPRECATED: use duckdb_value_string_internal instead. This function does not work correctly if the string contains
null bytes.
* returns: The char* value at the specified location. ONLY works on VARCHAR columns and does not auto-cast.
If the column is NOT a VARCHAR column this function will return NULL.

The result must NOT be freed.
*/
DUCKDB_API duckdb_string duckdb_value_string_internal(duckdb_result *result, idx_t col, idx_t row);

/*!
* returns: The duckdb_blob value at the specified location. Returns a blob with blob.data set to nullptr if the
value cannot be converted. The resulting "blob.data" must be freed with `duckdb_free.`
*/
DUCKDB_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);

/*!
 * returns: Returns true if the value at the specified index is NULL, and false otherwise.
 */
DUCKDB_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
/*!
Allocate `size` bytes of memory using the duckdb internal malloc function. Any memory allocated in this manner
should be freed using `duckdb_free`.

* size: The number of bytes to allocate.
* returns: A pointer to the allocated memory region.
*/
DUCKDB_API void *duckdb_malloc(size_t size);

/*!
Free a value returned from `duckdb_malloc`, `duckdb_value_varchar` or `duckdb_value_blob`.

* ptr: The memory region to de-allocate.
*/
DUCKDB_API void duckdb_free(void *ptr);

/*!
The internal vector size used by DuckDB.
This is the amount of tuples that will fit into a data chunk created by `duckdb_create_data_chunk`.

* returns: The vector size.
*/
DUCKDB_API idx_t duckdb_vector_size();

//===--------------------------------------------------------------------===//
// Date/Time/Timestamp Helpers
//===--------------------------------------------------------------------===//
/*!
Decompose a `duckdb_date` object into year, month and date (stored as `duckdb_date_struct`).

* date: The date object, as obtained from a `DUCKDB_TYPE_DATE` column.
* returns: The `duckdb_date_struct` with the decomposed elements.
*/
DUCKDB_API duckdb_date_struct duckdb_from_date(duckdb_date date);

/*!
Re-compose a `duckdb_date` from year, month and date (`duckdb_date_struct`).

* date: The year, month and date stored in a `duckdb_date_struct`.
* returns: The `duckdb_date` element.
*/
DUCKDB_API duckdb_date duckdb_to_date(duckdb_date_struct date);

/*!
Decompose a `duckdb_time` object into hour, minute, second and microsecond (stored as `duckdb_time_struct`).

* time: The time object, as obtained from a `DUCKDB_TYPE_TIME` column.
* returns: The `duckdb_time_struct` with the decomposed elements.
*/
DUCKDB_API duckdb_time_struct duckdb_from_time(duckdb_time time);

/*!
Re-compose a `duckdb_time` from hour, minute, second and microsecond (`duckdb_time_struct`).

* time: The hour, minute, second and microsecond in a `duckdb_time_struct`.
* returns: The `duckdb_time` element.
*/
DUCKDB_API duckdb_time duckdb_to_time(duckdb_time_struct time);

/*!
Decompose a `duckdb_timestamp` object into a `duckdb_timestamp_struct`.

* ts: The ts object, as obtained from a `DUCKDB_TYPE_TIMESTAMP` column.
* returns: The `duckdb_timestamp_struct` with the decomposed elements.
*/
DUCKDB_API duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts);

/*!
Re-compose a `duckdb_timestamp` from a duckdb_timestamp_struct.

* ts: The de-composed elements in a `duckdb_timestamp_struct`.
* returns: The `duckdb_timestamp` element.
*/
DUCKDB_API duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts);

//===--------------------------------------------------------------------===//
// Hugeint Helpers
//===--------------------------------------------------------------------===//
/*!
Converts a duckdb_hugeint object (as obtained from a `DUCKDB_TYPE_HUGEINT` column) into a double.

* val: The hugeint value.
* returns: The converted `double` element.
*/
DUCKDB_API double duckdb_hugeint_to_double(duckdb_hugeint val);

/*!
Converts a double value to a duckdb_hugeint object.

If the conversion fails because the double value is too big the result will be 0.

* val: The double value.
* returns: The converted `duckdb_hugeint` element.
*/
DUCKDB_API duckdb_hugeint duckdb_double_to_hugeint(double val);

/*!
Converts a double value to a duckdb_decimal object.

If the conversion fails because the double value is too big, or the width/scale are invalid the result will be 0.

* val: The double value.
* returns: The converted `duckdb_decimal` element.
*/
DUCKDB_API duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale);

//===--------------------------------------------------------------------===//
// Decimal Helpers
//===--------------------------------------------------------------------===//
/*!
Converts a duckdb_decimal object (as obtained from a `DUCKDB_TYPE_DECIMAL` column) into a double.

* val: The decimal value.
* returns: The converted `double` element.
*/
DUCKDB_API double duckdb_decimal_to_double(duckdb_decimal val);

//===--------------------------------------------------------------------===//
// Prepared Statements
//===--------------------------------------------------------------------===//
// A prepared statement is a parameterized query that allows you to bind parameters to it.
// * This is useful to easily supply parameters to functions and avoid SQL injection attacks.
// * This is useful to speed up queries that you will execute several times with different parameters.
// Because the query will only be parsed, bound, optimized and planned once during the prepare stage,
// rather than once per execution.
// For example:
//   SELECT * FROM tbl WHERE id=?
// Or a query with multiple parameters:
//   SELECT * FROM tbl WHERE id=$1 OR name=$2

/*!
Create a prepared statement object from a query.

Note that after calling `duckdb_prepare`, the prepared statement should always be destroyed using
`duckdb_destroy_prepare`, even if the prepare fails.

If the prepare fails, `duckdb_prepare_error` can be called to obtain the reason why the prepare failed.

* connection: The connection object
* query: The SQL query to prepare
* out_prepared_statement: The resulting prepared statement object
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                                       duckdb_prepared_statement *out_prepared_statement);

/*!
Closes the prepared statement and de-allocates all memory allocated for the statement.

* prepared_statement: The prepared statement to destroy.
*/
DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);

/*!
Returns the error message associated with the given prepared statement.
If the prepared statement has no error message, this returns `nullptr` instead.

The error message should not be freed. It will be de-allocated when `duckdb_destroy_prepare` is called.

* prepared_statement: The prepared statement to obtain the error from.
* returns: The error message, or `nullptr` if there is none.
*/
DUCKDB_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement);

/*!
Returns the number of parameters that can be provided to the given prepared statement.

Returns 0 if the query was not successfully prepared.

* prepared_statement: The prepared statement to obtain the number of parameters for.
*/
DUCKDB_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement);

/*!
Returns the parameter type for the parameter at the given index.

Returns `DUCKDB_TYPE_INVALID` if the parameter index is out of range or the statement was not successfully prepared.

* prepared_statement: The prepared statement.
* param_idx: The parameter index.
* returns: The parameter type
*/
DUCKDB_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);

/*!
Clear the params bind to the prepared statement.
*/
DUCKDB_API duckdb_state duckdb_clear_bindings(duckdb_prepared_statement prepared_statement);

/*!
Binds a bool value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);

/*!
Binds an int8_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);

/*!
Binds an int16_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);

/*!
Binds an int32_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);

/*!
Binds an int64_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);

/*!
Binds an duckdb_hugeint value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            duckdb_hugeint val);
/*!
Binds a duckdb_decimal value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_decimal(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            duckdb_decimal val);

/*!
Binds an uint8_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);

/*!
Binds an uint16_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);

/*!
Binds an uint32_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);

/*!
Binds an uint64_t value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);

/*!
Binds an float value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);

/*!
Binds an double value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);

/*!
Binds a duckdb_date value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         duckdb_date val);

/*!
Binds a duckdb_time value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         duckdb_time val);

/*!
Binds a duckdb_timestamp value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                              duckdb_timestamp val);

/*!
Binds a duckdb_interval value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                             duckdb_interval val);

/*!
Binds a null-terminated varchar value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            const char *val);

/*!
Binds a varchar value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                   const char *val, idx_t length);

/*!
Binds a blob value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         const void *data, idx_t length);

/*!
Binds a NULL value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);

/*!
Executes the prepared statement with the given bound parameters, and returns a materialized query result.

This method can be called multiple times for each prepared statement, and the parameters can be modified
between calls to this function.

* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
                                                duckdb_result *out_result);

/*!
Executes the prepared statement with the given bound parameters, and returns an arrow query result.

* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement,
                                                      duckdb_arrow *out_result);

//===--------------------------------------------------------------------===//
// Pending Result Interface
//===--------------------------------------------------------------------===//
/*!
Executes the prepared statement with the given bound parameters, and returns a pending result.
The pending result represents an intermediate structure for a query that is not yet fully executed.
The pending result can be used to incrementally execute a query, returning control to the client between tasks.

Note that after calling `duckdb_pending_prepared`, the pending result should always be destroyed using
`duckdb_destroy_pending`, even if this function returns DuckDBError.

* prepared_statement: The prepared statement to execute.
* out_result: The pending query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_pending_prepared(duckdb_prepared_statement prepared_statement,
                                                duckdb_pending_result *out_result);

/*!
Closes the pending result and de-allocates all memory allocated for the result.

* pending_result: The pending result to destroy.
*/
DUCKDB_API void duckdb_destroy_pending(duckdb_pending_result *pending_result);

/*!
Returns the error message contained within the pending result.

The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_pending` is called.

* result: The pending result to fetch the error from.
* returns: The error of the pending result.
*/
DUCKDB_API const char *duckdb_pending_error(duckdb_pending_result pending_result);

/*!
Executes a single task within the query, returning whether or not the query is ready.

If this returns DUCKDB_PENDING_RESULT_READY, the duckdb_execute_pending function can be called to obtain the result.
If this returns DUCKDB_PENDING_RESULT_NOT_READY, the duckdb_pending_execute_task function should be called again.
If this returns DUCKDB_PENDING_ERROR, an error occurred during execution.

The error message can be obtained by calling duckdb_pending_error on the pending_result.

* pending_result: The pending result to execute a task within..
* returns: The state of the pending result after the execution.
*/
DUCKDB_API duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result);

/*!
Fully execute a pending query result, returning the final query result.

If duckdb_pending_execute_task has been called until DUCKDB_PENDING_RESULT_READY was returned, this will return fast.
Otherwise, all remaining tasks must be executed first.

* pending_result: The pending result to execute.
* out_result: The result object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result);

//===--------------------------------------------------------------------===//
// Value Interface
//===--------------------------------------------------------------------===//
/*!
Destroys the value and de-allocates all memory allocated for that type.

* value: The value to destroy.
*/
DUCKDB_API void duckdb_destroy_value(duckdb_value *value);

/*!
Creates a value from a null-terminated string

* value: The null-terminated string
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_varchar(const char *text);

/*!
Creates a value from a string

* value: The text
* length: The length of the text
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_varchar_length(const char *text, idx_t length);

/*!
Creates a value from an int64

* value: The bigint value
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_int64(int64_t val);

/*!
Obtains a string representation of the given value.
The result must be destroyed with `duckdb_free`.

* value: The value
* returns: The string value. This must be destroyed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_get_varchar(duckdb_value value);

/*!
Obtains an int64 of the given value.

* value: The value
* returns: The int64 value, or 0 if no conversion is possible
*/
DUCKDB_API int64_t duckdb_get_int64(duckdb_value value);

//===--------------------------------------------------------------------===//
// Logical Type Interface
//===--------------------------------------------------------------------===//

/*!
Creates a `duckdb_logical_type` from a standard primitive type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

This should not be used with `DUCKDB_TYPE_DECIMAL`.

* type: The primitive type to create.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_logical_type(duckdb_type type);

/*!
Creates a list type from its child type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* type: The child type of list type to create.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type);

/*!
Creates a map type from its key type and value type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* type: The key type and value type of map type to create.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type);

/*!
Creates a `duckdb_logical_type` of type decimal with the specified width and scale
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* width: The width of the decimal type
* scale: The scale of the decimal type
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale);

/*!
Retrieves the type class of a `duckdb_logical_type`.

* type: The logical type object
* returns: The type id
*/
DUCKDB_API duckdb_type duckdb_get_type_id(duckdb_logical_type type);

/*!
Retrieves the width of a decimal type.

* type: The logical type object
* returns: The width of the decimal type
*/
DUCKDB_API uint8_t duckdb_decimal_width(duckdb_logical_type type);

/*!
Retrieves the scale of a decimal type.

* type: The logical type object
* returns: The scale of the decimal type
*/
DUCKDB_API uint8_t duckdb_decimal_scale(duckdb_logical_type type);

/*!
Retrieves the internal storage type of a decimal type.

* type: The logical type object
* returns: The internal type of the decimal type
*/
DUCKDB_API duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type);

/*!
Retrieves the internal storage type of an enum type.

* type: The logical type object
* returns: The internal type of the enum type
*/
DUCKDB_API duckdb_type duckdb_enum_internal_type(duckdb_logical_type type);

/*!
Retrieves the dictionary size of the enum type

* type: The logical type object
* returns: The dictionary size of the enum type
*/
DUCKDB_API uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type);

/*!
Retrieves the dictionary value at the specified position from the enum.

The result must be freed with `duckdb_free`

* type: The logical type object
* index: The index in the dictionary
* returns: The string value of the enum type. Must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index);

/*!
Retrieves the child type of the given list type.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* returns: The child type of the list type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);

/*!
Retrieves the key type of the given map type.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* returns: The key type of the map type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type);

/*!
Retrieves the value type of the given map type.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* returns: The value type of the map type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type);

/*!
Returns the number of children of a struct type.

* type: The logical type object
* returns: The number of children of a struct type.
*/
DUCKDB_API idx_t duckdb_struct_type_child_count(duckdb_logical_type type);

/*!
Retrieves the name of the struct child.

The result must be freed with `duckdb_free`

* type: The logical type object
* index: The child index
* returns: The name of the struct type. Must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index);

/*!
Retrieves the child type of the given struct type at the specified index.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* index: The child index
* returns: The child type of the struct type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index);

/*!
Destroys the logical type and de-allocates all memory allocated for that type.

* type: The logical type to destroy.
*/
DUCKDB_API void duckdb_destroy_logical_type(duckdb_logical_type *type);

//===--------------------------------------------------------------------===//
// Data Chunk Interface
//===--------------------------------------------------------------------===//
/*!
Creates an empty DataChunk with the specified set of types.

* types: An array of types of the data chunk.
* column_count: The number of columns.
* returns: The data chunk.
*/
DUCKDB_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);

/*!
Destroys the data chunk and de-allocates all memory allocated for that chunk.

* chunk: The data chunk to destroy.
*/
DUCKDB_API void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);

/*!
Resets a data chunk, clearing the validity masks and setting the cardinality of the data chunk to 0.

* chunk: The data chunk to reset.
*/
DUCKDB_API void duckdb_data_chunk_reset(duckdb_data_chunk chunk);

/*!
Retrieves the number of columns in a data chunk.

* chunk: The data chunk to get the data from
* returns: The number of columns in the data chunk
*/
DUCKDB_API idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);

/*!
Retrieves the vector at the specified column index in the data chunk.

The pointer to the vector is valid for as long as the chunk is alive.
It does NOT need to be destroyed.

* chunk: The data chunk to get the data from
* returns: The vector
*/
DUCKDB_API duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);

/*!
Retrieves the current number of tuples in a data chunk.

* chunk: The data chunk to get the data from
* returns: The number of tuples in the data chunk
*/
DUCKDB_API idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);

/*!
Sets the current number of tuples in a data chunk.

* chunk: The data chunk to set the size in
* size: The number of tuples in the data chunk
*/
DUCKDB_API void duckdb_data_chunk_set_size(duckdb_data_chunk chunk, idx_t size);

//===--------------------------------------------------------------------===//
// Vector Interface
//===--------------------------------------------------------------------===//
/*!
Retrieves the column type of the specified vector.

The result must be destroyed with `duckdb_destroy_logical_type`.

* vector: The vector get the data from
* returns: The type of the vector
*/
DUCKDB_API duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector);

/*!
Retrieves the data pointer of the vector.

The data pointer can be used to read or write values from the vector.
How to read or write values depends on the type of the vector.

* vector: The vector to get the data from
* returns: The data pointer
*/
DUCKDB_API void *duckdb_vector_get_data(duckdb_vector vector);

/*!
Retrieves the validity mask pointer of the specified vector.

If all values are valid, this function MIGHT return NULL!

The validity mask is a bitset that signifies null-ness within the data chunk.
It is a series of uint64_t values, where each uint64_t value contains validity for 64 tuples.
The bit is set to 1 if the value is valid (i.e. not NULL) or 0 if the value is invalid (i.e. NULL).

Validity of a specific value can be obtained like this:

idx_t entry_idx = row_idx / 64;
idx_t idx_in_entry = row_idx % 64;
bool is_valid = validity_mask[entry_idx] & (1 << idx_in_entry);

Alternatively, the (slower) duckdb_validity_row_is_valid function can be used.

* vector: The vector to get the data from
* returns: The pointer to the validity mask, or NULL if no validity mask is present
*/
DUCKDB_API uint64_t *duckdb_vector_get_validity(duckdb_vector vector);

/*!
Ensures the validity mask is writable by allocating it.

After this function is called, `duckdb_vector_get_validity` will ALWAYS return non-NULL.
This allows null values to be written to the vector, regardless of whether a validity mask was present before.

* vector: The vector to alter
*/
DUCKDB_API void duckdb_vector_ensure_validity_writable(duckdb_vector vector);

/*!
Assigns a string element in the vector at the specified location.

* vector: The vector to alter
* index: The row position in the vector to assign the string to
* str: The null-terminated string
*/
DUCKDB_API void duckdb_vector_assign_string_element(duckdb_vector vector, idx_t index, const char *str);

/*!
Assigns a string element in the vector at the specified location.

* vector: The vector to alter
* index: The row position in the vector to assign the string to
* str: The string
* str_len: The length of the string (in bytes)
*/
DUCKDB_API void duckdb_vector_assign_string_element_len(duckdb_vector vector, idx_t index, const char *str,
                                                        idx_t str_len);

/*!
Retrieves the child vector of a list vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* returns: The child vector
*/
DUCKDB_API duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector);

/*!
Returns the size of the child vector of the list

* vector: The vector
* returns: The size of the child list
*/
DUCKDB_API idx_t duckdb_list_vector_get_size(duckdb_vector vector);

/*!
Retrieves the child vector of a struct vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* index: The child index
* returns: The child vector
*/
DUCKDB_API duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index);

//===--------------------------------------------------------------------===//
// Validity Mask Functions
//===--------------------------------------------------------------------===//
/*!
Returns whether or not a row is valid (i.e. not NULL) in the given validity mask.

* validity: The validity mask, as obtained through `duckdb_data_chunk_get_validity`
* row: The row index
* returns: true if the row is valid, false otherwise
*/
DUCKDB_API bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row);

/*!
In a validity mask, sets a specific row to either valid or invalid.

Note that `duckdb_data_chunk_ensure_validity_writable` should be called before calling `duckdb_data_chunk_get_validity`,
to ensure that there is a validity mask to write to.

* validity: The validity mask, as obtained through `duckdb_data_chunk_get_validity`.
* row: The row index
* valid: Whether or not to set the row to valid, or invalid
*/
DUCKDB_API void duckdb_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid);

/*!
In a validity mask, sets a specific row to invalid.

Equivalent to `duckdb_validity_set_row_validity` with valid set to false.

* validity: The validity mask
* row: The row index
*/
DUCKDB_API void duckdb_validity_set_row_invalid(uint64_t *validity, idx_t row);

/*!
In a validity mask, sets a specific row to valid.

Equivalent to `duckdb_validity_set_row_validity` with valid set to true.

* validity: The validity mask
* row: The row index
*/
DUCKDB_API void duckdb_validity_set_row_valid(uint64_t *validity, idx_t row);

//===--------------------------------------------------------------------===//
// Table Functions
//===--------------------------------------------------------------------===//
typedef void *duckdb_table_function;
typedef void *duckdb_bind_info;
typedef void *duckdb_init_info;
typedef void *duckdb_function_info;

typedef void (*duckdb_table_function_bind_t)(duckdb_bind_info info);
typedef void (*duckdb_table_function_init_t)(duckdb_init_info info);
typedef void (*duckdb_table_function_t)(duckdb_function_info info, duckdb_data_chunk output);
typedef void (*duckdb_delete_callback_t)(void *data);

/*!
Creates a new empty table function.

The return value should be destroyed with `duckdb_destroy_table_function`.

* returns: The table function object.
*/
DUCKDB_API duckdb_table_function duckdb_create_table_function();

/*!
Destroys the given table function object.

* table_function: The table function to destroy
*/
DUCKDB_API void duckdb_destroy_table_function(duckdb_table_function *table_function);

/*!
Sets the name of the given table function.

* table_function: The table function
* name: The name of the table function
*/
DUCKDB_API void duckdb_table_function_set_name(duckdb_table_function table_function, const char *name);

/*!
Adds a parameter to the table function.

* table_function: The table function
* type: The type of the parameter to add.
*/
DUCKDB_API void duckdb_table_function_add_parameter(duckdb_table_function table_function, duckdb_logical_type type);

/*!
Assigns extra information to the table function that can be fetched during binding, etc.

* table_function: The table function
* extra_info: The extra information
* destroy: The callback that will be called to destroy the bind data (if any)
*/
DUCKDB_API void duckdb_table_function_set_extra_info(duckdb_table_function table_function, void *extra_info,
                                                     duckdb_delete_callback_t destroy);

/*!
Sets the bind function of the table function

* table_function: The table function
* bind: The bind function
*/
DUCKDB_API void duckdb_table_function_set_bind(duckdb_table_function table_function, duckdb_table_function_bind_t bind);

/*!
Sets the init function of the table function

* table_function: The table function
* init: The init function
*/
DUCKDB_API void duckdb_table_function_set_init(duckdb_table_function table_function, duckdb_table_function_init_t init);

/*!
Sets the thread-local init function of the table function

* table_function: The table function
* init: The init function
*/
DUCKDB_API void duckdb_table_function_set_local_init(duckdb_table_function table_function,
                                                     duckdb_table_function_init_t init);

/*!
Sets the main function of the table function

* table_function: The table function
* function: The function
*/
DUCKDB_API void duckdb_table_function_set_function(duckdb_table_function table_function,
                                                   duckdb_table_function_t function);

/*!
Sets whether or not the given table function supports projection pushdown.

If this is set to true, the system will provide a list of all required columns in the `init` stage through
the `duckdb_init_get_column_count` and `duckdb_init_get_column_index` functions.
If this is set to false (the default), the system will expect all columns to be projected.

* table_function: The table function
* pushdown: True if the table function supports projection pushdown, false otherwise.
*/
DUCKDB_API void duckdb_table_function_supports_projection_pushdown(duckdb_table_function table_function, bool pushdown);

/*!
Register the table function object within the given connection.

The function requires at least a name, a bind function, an init function and a main function.

If the function is incomplete or a function with this name already exists DuckDBError is returned.

* con: The connection to register it in.
* function: The function pointer
* returns: Whether or not the registration was successful.
*/
DUCKDB_API duckdb_state duckdb_register_table_function(duckdb_connection con, duckdb_table_function function);

//===--------------------------------------------------------------------===//
// Table Function Bind
//===--------------------------------------------------------------------===//
/*!
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
*/
DUCKDB_API void *duckdb_bind_get_extra_info(duckdb_bind_info info);

/*!
Adds a result column to the output of the table function.

* info: The info object
* name: The name of the column
* type: The logical type of the column
*/
DUCKDB_API void duckdb_bind_add_result_column(duckdb_bind_info info, const char *name, duckdb_logical_type type);

/*!
Retrieves the number of regular (non-named) parameters to the function.

* info: The info object
* returns: The number of parameters
*/
DUCKDB_API idx_t duckdb_bind_get_parameter_count(duckdb_bind_info info);

/*!
Retrieves the parameter at the given index.

The result must be destroyed with `duckdb_destroy_value`.

* info: The info object
* index: The index of the parameter to get
* returns: The value of the parameter. Must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_bind_get_parameter(duckdb_bind_info info, idx_t index);

/*!
Sets the user-provided bind data in the bind object. This object can be retrieved again during execution.

* info: The info object
* extra_data: The bind data object.
* destroy: The callback that will be called to destroy the bind data (if any)
*/
DUCKDB_API void duckdb_bind_set_bind_data(duckdb_bind_info info, void *bind_data, duckdb_delete_callback_t destroy);

/*!
Sets the cardinality estimate for the table function, used for optimization.

* info: The bind data object.
* is_exact: Whether or not the cardinality estimate is exact, or an approximation
*/
DUCKDB_API void duckdb_bind_set_cardinality(duckdb_bind_info info, idx_t cardinality, bool is_exact);

/*!
Report that an error has occurred while calling bind.

* info: The info object
* error: The error message
*/
DUCKDB_API void duckdb_bind_set_error(duckdb_bind_info info, const char *error);

//===--------------------------------------------------------------------===//
// Table Function Init
//===--------------------------------------------------------------------===//

/*!
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
*/
DUCKDB_API void *duckdb_init_get_extra_info(duckdb_init_info info);

/*!
Gets the bind data set by `duckdb_bind_set_bind_data` during the bind.

Note that the bind data should be considered as read-only.
For tracking state, use the init data instead.

* info: The info object
* returns: The bind data object
*/
DUCKDB_API void *duckdb_init_get_bind_data(duckdb_init_info info);

/*!
Sets the user-provided init data in the init object. This object can be retrieved again during execution.

* info: The info object
* extra_data: The init data object.
* destroy: The callback that will be called to destroy the init data (if any)
*/
DUCKDB_API void duckdb_init_set_init_data(duckdb_init_info info, void *init_data, duckdb_delete_callback_t destroy);

/*!
Returns the number of projected columns.

This function must be used if projection pushdown is enabled to figure out which columns to emit.

* info: The info object
* returns: The number of projected columns.
*/
DUCKDB_API idx_t duckdb_init_get_column_count(duckdb_init_info info);

/*!
Returns the column index of the projected column at the specified position.

This function must be used if projection pushdown is enabled to figure out which columns to emit.

* info: The info object
* column_index: The index at which to get the projected column index, from 0..duckdb_init_get_column_count(info)
* returns: The column index of the projected column.
*/
DUCKDB_API idx_t duckdb_init_get_column_index(duckdb_init_info info, idx_t column_index);

/*!
Sets how many threads can process this table function in parallel (default: 1)

* info: The info object
* max_threads: The maximum amount of threads that can process this table function
*/
DUCKDB_API void duckdb_init_set_max_threads(duckdb_init_info info, idx_t max_threads);

/*!
Report that an error has occurred while calling init.

* info: The info object
* error: The error message
*/
DUCKDB_API void duckdb_init_set_error(duckdb_init_info info, const char *error);

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//

/*!
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
*/
DUCKDB_API void *duckdb_function_get_extra_info(duckdb_function_info info);
/*!
Gets the bind data set by `duckdb_bind_set_bind_data` during the bind.

Note that the bind data should be considered as read-only.
For tracking state, use the init data instead.

* info: The info object
* returns: The bind data object
*/
DUCKDB_API void *duckdb_function_get_bind_data(duckdb_function_info info);

/*!
Gets the init data set by `duckdb_init_set_init_data` during the init.

* info: The info object
* returns: The init data object
*/
DUCKDB_API void *duckdb_function_get_init_data(duckdb_function_info info);

/*!
Gets the thread-local init data set by `duckdb_init_set_init_data` during the local_init.

* info: The info object
* returns: The init data object
*/
DUCKDB_API void *duckdb_function_get_local_init_data(duckdb_function_info info);

/*!
Report that an error has occurred while executing the function.

* info: The info object
* error: The error message
*/
DUCKDB_API void duckdb_function_set_error(duckdb_function_info info, const char *error);

//===--------------------------------------------------------------------===//
// Replacement Scans
//===--------------------------------------------------------------------===//
typedef void *duckdb_replacement_scan_info;

typedef void (*duckdb_replacement_callback_t)(duckdb_replacement_scan_info info, const char *table_name, void *data);

/*!
Add a replacement scan definition to the specified database

* db: The database object to add the replacement scan to
* replacement: The replacement scan callback
* extra_data: Extra data that is passed back into the specified callback
* delete_callback: The delete callback to call on the extra data, if any
*/
DUCKDB_API void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement,
                                            void *extra_data, duckdb_delete_callback_t delete_callback);

/*!
Sets the replacement function name to use. If this function is called in the replacement callback,
 the replacement scan is performed. If it is not called, the replacement callback is not performed.

* info: The info object
* function_name: The function name to substitute.
*/
DUCKDB_API void duckdb_replacement_scan_set_function_name(duckdb_replacement_scan_info info, const char *function_name);

/*!
Adds a parameter to the replacement scan function.

* info: The info object
* parameter: The parameter to add.
*/
DUCKDB_API void duckdb_replacement_scan_add_parameter(duckdb_replacement_scan_info info, duckdb_value parameter);

/*!
Report that an error has occurred while executing the replacement scan.

* info: The info object
* error: The error message
*/
DUCKDB_API void duckdb_replacement_scan_set_error(duckdb_replacement_scan_info info, const char *error);

//===--------------------------------------------------------------------===//
// Appender
//===--------------------------------------------------------------------===//

// Appenders are the most efficient way of loading data into DuckDB from within the C interface, and are recommended for
// fast data loading. The appender is much faster than using prepared statements or individual `INSERT INTO` statements.

// Appends are made in row-wise format. For every column, a `duckdb_append_[type]` call should be made, after which
// the row should be finished by calling `duckdb_appender_end_row`. After all rows have been appended,
// `duckdb_appender_destroy` should be used to finalize the appender and clean up the resulting memory.

// Note that `duckdb_appender_destroy` should always be called on the resulting appender, even if the function returns
// `DuckDBError`.

/*!
Creates an appender object.

* connection: The connection context to create the appender in.
* schema: The schema of the table to append to, or `nullptr` for the default schema.
* table: The table name to append to.
* out_appender: The resulting appender object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                               duckdb_appender *out_appender);

/*!
Returns the error message associated with the given appender.
If the appender has no error message, this returns `nullptr` instead.

The error message should not be freed. It will be de-allocated when `duckdb_appender_destroy` is called.

* appender: The appender to get the error from.
* returns: The error message, or `nullptr` if there is none.
*/
DUCKDB_API const char *duckdb_appender_error(duckdb_appender appender);

/*!
Flush the appender to the table, forcing the cache of the appender to be cleared and the data to be appended to the
base table.

This should generally not be used unless you know what you are doing. Instead, call `duckdb_appender_destroy` when you
are done with the appender.

* appender: The appender to flush.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_flush(duckdb_appender appender);

/*!
Close the appender, flushing all intermediate state in the appender to the table and closing it for further appends.

This is generally not necessary. Call `duckdb_appender_destroy` instead.

* appender: The appender to flush and close.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_close(duckdb_appender appender);

/*!
Close the appender and destroy it. Flushing all intermediate state in the appender to the table, and de-allocating
all memory associated with the appender.

* appender: The appender to flush, close and destroy.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_destroy(duckdb_appender *appender);

/*!
A nop function, provided for backwards compatibility reasons. Does nothing. Only `duckdb_appender_end_row` is required.
*/
DUCKDB_API duckdb_state duckdb_appender_begin_row(duckdb_appender appender);

/*!
Finish the current row of appends. After end_row is called, the next row can be appended.

* appender: The appender.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_end_row(duckdb_appender appender);

/*!
Append a bool value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);

/*!
Append an int8_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value);
/*!
Append an int16_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value);
/*!
Append an int32_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
/*!
Append an int64_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);
/*!
Append a duckdb_hugeint value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value);

/*!
Append a uint8_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value);
/*!
Append a uint16_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value);
/*!
Append a uint32_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value);
/*!
Append a uint64_t value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value);

/*!
Append a float value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_float(duckdb_appender appender, float value);
/*!
Append a double value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_double(duckdb_appender appender, double value);

/*!
Append a duckdb_date value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value);
/*!
Append a duckdb_time value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value);
/*!
Append a duckdb_timestamp value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value);
/*!
Append a duckdb_interval value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value);

/*!
Append a varchar value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val);
/*!
Append a varchar value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length);
/*!
Append a blob value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length);
/*!
Append a NULL value to the appender (of any type).
*/
DUCKDB_API duckdb_state duckdb_append_null(duckdb_appender appender);

/*!
Appends a pre-filled data chunk to the specified appender.

The types of the data chunk must exactly match the types of the table, no casting is performed.
If the types do not match or the appender is in an invalid state, DuckDBError is returned.
If the append is successful, DuckDBSuccess is returned.

* appender: The appender to append to.
* chunk: The data chunk to append.
* returns: The return state.
*/
DUCKDB_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);

//===--------------------------------------------------------------------===//
// Arrow Interface
//===--------------------------------------------------------------------===//
/*!
Executes a SQL query within a connection and stores the full (materialized) result in an arrow structure.
If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
`duckdb_query_arrow_error`.

Note that after running `duckdb_query_arrow`, `duckdb_destroy_arrow` must be called on the result object even if the
query fails, otherwise the error stored within the result will not be freed correctly.

* connection: The connection to perform the query in.
* query: The SQL query to run.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result);

/*!
Fetch the internal arrow schema from the arrow result.

* result: The result to fetch the schema from.
* out_schema: The output schema.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema);

/*!
Fetch an internal arrow array from the arrow result.

This function can be called multiple time to get next chunks, which will free the previous out_array.
So consume the out_array before calling this function again.

* result: The result to fetch the array from.
* out_array: The output array.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array);

/*!
Returns the number of columns present in a the arrow result object.

* result: The result object.
* returns: The number of columns present in the result object.
*/
DUCKDB_API idx_t duckdb_arrow_column_count(duckdb_arrow result);

/*!
Returns the number of rows present in a the arrow result object.

* result: The result object.
* returns: The number of rows present in the result object.
*/
DUCKDB_API idx_t duckdb_arrow_row_count(duckdb_arrow result);

/*!
Returns the number of rows changed by the query stored in the arrow result. This is relevant only for
INSERT/UPDATE/DELETE queries. For other queries the rows_changed will be 0.

* result: The result object.
* returns: The number of rows changed.
*/
DUCKDB_API idx_t duckdb_arrow_rows_changed(duckdb_arrow result);

/*!
Returns the error message contained within the result. The error is only set if `duckdb_query_arrow` returns
`DuckDBError`.

The error message should not be freed. It will be de-allocated when `duckdb_destroy_arrow` is called.

* result: The result object to fetch the nullmask from.
* returns: The error of the result.
*/
DUCKDB_API const char *duckdb_query_arrow_error(duckdb_arrow result);

/*!
Closes the result and de-allocates all memory allocated for the arrow result.

* result: The result to destroy.
*/
DUCKDB_API void duckdb_destroy_arrow(duckdb_arrow *result);

//===--------------------------------------------------------------------===//
// Threading Information
//===--------------------------------------------------------------------===//
typedef void *duckdb_task_state;

/*!
Execute DuckDB tasks on this thread.

Will return after `max_tasks` have been executed, or if there are no more tasks present.

* database: The database object to execute tasks for
* max_tasks: The maximum amount of tasks to execute
*/
DUCKDB_API void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks);

/*!
Creates a task state that can be used with duckdb_execute_tasks_state to execute tasks until
 duckdb_finish_execution is called on the state.

duckdb_destroy_state should be called on the result in order to free memory.

* database: The database object to create the task state for
* returns: The task state that can be used with duckdb_execute_tasks_state.
*/
DUCKDB_API duckdb_task_state duckdb_create_task_state(duckdb_database database);

/*!
Execute DuckDB tasks on this thread.

The thread will keep on executing tasks forever, until duckdb_finish_execution is called on the state.
Multiple threads can share the same duckdb_task_state.

* state: The task state of the executor
*/
DUCKDB_API void duckdb_execute_tasks_state(duckdb_task_state state);

/*!
Execute DuckDB tasks on this thread.

The thread will keep on executing tasks until either duckdb_finish_execution is called on the state,
max_tasks tasks have been executed or there are no more tasks to be executed.

Multiple threads can share the same duckdb_task_state.

* state: The task state of the executor
* max_tasks: The maximum amount of tasks to execute
* returns: The amount of tasks that have actually been executed
*/
DUCKDB_API idx_t duckdb_execute_n_tasks_state(duckdb_task_state state, idx_t max_tasks);

/*!
Finish execution on a specific task.

* state: The task state to finish execution
*/
DUCKDB_API void duckdb_finish_execution(duckdb_task_state state);

/*!
Check if the provided duckdb_task_state has finished execution

* state: The task state to inspect
* returns: Whether or not duckdb_finish_execution has been called on the task state
*/
DUCKDB_API bool duckdb_task_state_is_finished(duckdb_task_state state);

/*!
Destroys the task state returned from duckdb_create_task_state.

Note that this should not be called while there is an active duckdb_execute_tasks_state running
on the task state.

* state: The task state to clean up
*/
DUCKDB_API void duckdb_destroy_task_state(duckdb_task_state state);

#ifdef __cplusplus
}
#endif
