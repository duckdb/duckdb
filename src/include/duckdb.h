//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

//! duplicate of duckdb/main/winapi.hpp
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

//! duplicate of duckdb/main/winapi.hpp
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

//! In the future, we are planning to move extension functions to a separate header. For now you can set the define
//! below to remove the functions that are planned to be moved out of this header.
// #define DUCKDB_NO_EXTENSION_FUNCTIONS

//! Set the define below to remove all functions that are deprecated or planned to be deprecated
// #define DUCKDB_API_NO_DEPRECATED

//! API versions
//! If no explicit API version is defined, the latest API version is used.
//! Note that using older API versions (i.e. not using DUCKDB_API_LATEST) is deprecated.
//! These will not be supported long-term, and will be removed in future versions.
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
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

//===--------------------------------------------------------------------===//
// Enums
//===--------------------------------------------------------------------===//
// WARNING: the numbers of these enums should not be changed, as changing the numbers breaks ABI compatibility
// Always add enums at the END of the enum
//! An enum over DuckDB's internal types.
typedef enum DUCKDB_TYPE {
	DUCKDB_TYPE_INVALID = 0,
	// bool
	DUCKDB_TYPE_BOOLEAN = 1,
	// int8_t
	DUCKDB_TYPE_TINYINT = 2,
	// int16_t
	DUCKDB_TYPE_SMALLINT = 3,
	// int32_t
	DUCKDB_TYPE_INTEGER = 4,
	// int64_t
	DUCKDB_TYPE_BIGINT = 5,
	// uint8_t
	DUCKDB_TYPE_UTINYINT = 6,
	// uint16_t
	DUCKDB_TYPE_USMALLINT = 7,
	// uint32_t
	DUCKDB_TYPE_UINTEGER = 8,
	// uint64_t
	DUCKDB_TYPE_UBIGINT = 9,
	// float
	DUCKDB_TYPE_FLOAT = 10,
	// double
	DUCKDB_TYPE_DOUBLE = 11,
	// duckdb_timestamp, in microseconds
	DUCKDB_TYPE_TIMESTAMP = 12,
	// duckdb_date
	DUCKDB_TYPE_DATE = 13,
	// duckdb_time
	DUCKDB_TYPE_TIME = 14,
	// duckdb_interval
	DUCKDB_TYPE_INTERVAL = 15,
	// duckdb_hugeint
	DUCKDB_TYPE_HUGEINT = 16,
	// duckdb_uhugeint
	DUCKDB_TYPE_UHUGEINT = 32,
	// const char*
	DUCKDB_TYPE_VARCHAR = 17,
	// duckdb_blob
	DUCKDB_TYPE_BLOB = 18,
	// decimal
	DUCKDB_TYPE_DECIMAL = 19,
	// duckdb_timestamp, in seconds
	DUCKDB_TYPE_TIMESTAMP_S = 20,
	// duckdb_timestamp, in milliseconds
	DUCKDB_TYPE_TIMESTAMP_MS = 21,
	// duckdb_timestamp, in nanoseconds
	DUCKDB_TYPE_TIMESTAMP_NS = 22,
	// enum type, only useful as logical type
	DUCKDB_TYPE_ENUM = 23,
	// list type, only useful as logical type
	DUCKDB_TYPE_LIST = 24,
	// struct type, only useful as logical type
	DUCKDB_TYPE_STRUCT = 25,
	// map type, only useful as logical type
	DUCKDB_TYPE_MAP = 26,
	// duckdb_array, only useful as logical type
	DUCKDB_TYPE_ARRAY = 33,
	// duckdb_hugeint
	DUCKDB_TYPE_UUID = 27,
	// union type, only useful as logical type
	DUCKDB_TYPE_UNION = 28,
	// duckdb_bit
	DUCKDB_TYPE_BIT = 29,
	// duckdb_time_tz
	DUCKDB_TYPE_TIME_TZ = 30,
	// duckdb_timestamp
	DUCKDB_TYPE_TIMESTAMP_TZ = 31,
} duckdb_type;
//! An enum over the returned state of different functions.
typedef enum duckdb_state { DuckDBSuccess = 0, DuckDBError = 1 } duckdb_state;
//! An enum over the pending state of a pending query result.
typedef enum duckdb_pending_state {
	DUCKDB_PENDING_RESULT_READY = 0,
	DUCKDB_PENDING_RESULT_NOT_READY = 1,
	DUCKDB_PENDING_ERROR = 2,
	DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3
} duckdb_pending_state;
//! An enum over DuckDB's different result types.
typedef enum duckdb_result_type {
	DUCKDB_RESULT_TYPE_INVALID = 0,
	DUCKDB_RESULT_TYPE_CHANGED_ROWS = 1,
	DUCKDB_RESULT_TYPE_NOTHING = 2,
	DUCKDB_RESULT_TYPE_QUERY_RESULT = 3,
} duckdb_result_type;
//! An enum over DuckDB's different statement types.
typedef enum duckdb_statement_type {
	DUCKDB_STATEMENT_TYPE_INVALID = 0,
	DUCKDB_STATEMENT_TYPE_SELECT = 1,
	DUCKDB_STATEMENT_TYPE_INSERT = 2,
	DUCKDB_STATEMENT_TYPE_UPDATE = 3,
	DUCKDB_STATEMENT_TYPE_EXPLAIN = 4,
	DUCKDB_STATEMENT_TYPE_DELETE = 5,
	DUCKDB_STATEMENT_TYPE_PREPARE = 6,
	DUCKDB_STATEMENT_TYPE_CREATE = 7,
	DUCKDB_STATEMENT_TYPE_EXECUTE = 8,
	DUCKDB_STATEMENT_TYPE_ALTER = 9,
	DUCKDB_STATEMENT_TYPE_TRANSACTION = 10,
	DUCKDB_STATEMENT_TYPE_COPY = 11,
	DUCKDB_STATEMENT_TYPE_ANALYZE = 12,
	DUCKDB_STATEMENT_TYPE_VARIABLE_SET = 13,
	DUCKDB_STATEMENT_TYPE_CREATE_FUNC = 14,
	DUCKDB_STATEMENT_TYPE_DROP = 15,
	DUCKDB_STATEMENT_TYPE_EXPORT = 16,
	DUCKDB_STATEMENT_TYPE_PRAGMA = 17,
	DUCKDB_STATEMENT_TYPE_VACUUM = 18,
	DUCKDB_STATEMENT_TYPE_CALL = 19,
	DUCKDB_STATEMENT_TYPE_SET = 20,
	DUCKDB_STATEMENT_TYPE_LOAD = 21,
	DUCKDB_STATEMENT_TYPE_RELATION = 22,
	DUCKDB_STATEMENT_TYPE_EXTENSION = 23,
	DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN = 24,
	DUCKDB_STATEMENT_TYPE_ATTACH = 25,
	DUCKDB_STATEMENT_TYPE_DETACH = 26,
	DUCKDB_STATEMENT_TYPE_MULTI = 27,
} duckdb_statement_type;

//===--------------------------------------------------------------------===//
// General type definitions
//===--------------------------------------------------------------------===//

//! DuckDB's index type.
typedef uint64_t idx_t;

//! The callback that will be called to destroy data, e.g.,
//! bind data (if any), init data (if any), extra data for replacement scans (if any)
typedef void (*duckdb_delete_callback_t)(void *data);

//! Used for threading, contains a task state. Must be destroyed with `duckdb_destroy_state`.
typedef void *duckdb_task_state;

//===--------------------------------------------------------------------===//
// Types (no explicit freeing)
//===--------------------------------------------------------------------===//

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

//! TIME_TZ is stored as 40 bits for int64_t micros, and 24 bits for int32_t offset
typedef struct {
	uint64_t bits;
} duckdb_time_tz;
typedef struct {
	duckdb_time_struct time;
	int32_t offset;
} duckdb_time_tz_struct;

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

//! Hugeints are composed of a (lower, upper) component
//! The value of the hugeint is upper * 2^64 + lower
//! For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended
typedef struct {
	uint64_t lower;
	int64_t upper;
} duckdb_hugeint;
typedef struct {
	uint64_t lower;
	uint64_t upper;
} duckdb_uhugeint;

//! Decimals are composed of a width and a scale, and are stored in a hugeint
typedef struct {
	uint8_t width;
	uint8_t scale;
	duckdb_hugeint value;
} duckdb_decimal;

//! A type holding information about the query execution progress
typedef struct {
	double percentage;
	uint64_t rows_processed;
	uint64_t total_rows_to_process;
} duckdb_query_progress_type;

//! The internal representation of a VARCHAR (string_t). If the VARCHAR does not
//! exceed 12 characters, then we inline it. Otherwise, we inline a prefix for faster
//! string comparisons and store a pointer to the remaining characters. This is a non-
//! owning structure, i.e., it does not have to be freed.
typedef struct {
	union {
		struct {
			uint32_t length;
			char prefix[4];
			char *ptr;
		} pointer;
		struct {
			uint32_t length;
			char inlined[12];
		} inlined;
	} value;
} duckdb_string_t;

//! The internal representation of a list metadata entry contains the list's offset in
//! the child vector, and its length. The parent vector holds these metadata entries,
//! whereas the child vector holds the data
typedef struct {
	uint64_t offset;
	uint64_t length;
} duckdb_list_entry;

//! A column consists of a pointer to its internal data. Don't operate on this type directly.
//! Instead, use functions such as duckdb_column_data, duckdb_nullmask_data,
//! duckdb_column_type, and duckdb_column_name, which take the result and the column index
//! as their parameters
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

//! A vector to a specified column in a data chunk. Lives as long as the
//! data chunk lives, i.e., must not be destroyed.
typedef struct _duckdb_vector {
	void *__vctr;
} * duckdb_vector;

//===--------------------------------------------------------------------===//
// Types (explicit freeing/destroying)
//===--------------------------------------------------------------------===//

//! Strings are composed of a char pointer and a size. You must free string.data
//! with `duckdb_free`.
typedef struct {
	char *data;
	idx_t size;
} duckdb_string;

//! BLOBs are composed of a byte pointer and a size. You must free blob.data
//! with `duckdb_free`.
typedef struct {
	void *data;
	idx_t size;
} duckdb_blob;

//! A query result consists of a pointer to its internal data.
//! Must be freed with 'duckdb_destroy_result'.
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
	// deprecated, use duckdb_column_*-family of functions
	duckdb_column *__deprecated_columns;
	// deprecated, use duckdb_result_error
	char *__deprecated_error_message;
#endif
	void *internal_data;
} duckdb_result;

//! A database object. Should be closed with `duckdb_close`.
typedef struct _duckdb_database {
	void *__db;
} * duckdb_database;

//! A connection to a duckdb database. Must be closed with `duckdb_disconnect`.
typedef struct _duckdb_connection {
	void *__conn;
} * duckdb_connection;

//! A prepared statement is a parameterized query that allows you to bind parameters to it.
//! Must be destroyed with `duckdb_destroy_prepare`.
typedef struct _duckdb_prepared_statement {
	void *__prep;
} * duckdb_prepared_statement;

//! Extracted statements. Must be destroyed with `duckdb_destroy_extracted`.
typedef struct _duckdb_extracted_statements {
	void *__extrac;
} * duckdb_extracted_statements;

//! The pending result represents an intermediate structure for a query that is not yet fully executed.
//! Must be destroyed with `duckdb_destroy_pending`.
typedef struct _duckdb_pending_result {
	void *__pend;
} * duckdb_pending_result;

//! The appender enables fast data loading into DuckDB.
//! Must be destroyed with `duckdb_appender_destroy`.
typedef struct _duckdb_appender {
	void *__appn;
} * duckdb_appender;

//! The table description allows querying info about the table.
//! Must be destroyed with `duckdb_table_description_destroy`.
typedef struct _duckdb_table_description {
	void *__tabledesc;
} * duckdb_table_description;

//! Can be used to provide start-up options for the DuckDB instance.
//! Must be destroyed with `duckdb_destroy_config`.
typedef struct _duckdb_config {
	void *__cnfg;
} * duckdb_config;

//! Holds an internal logical type.
//! Must be destroyed with `duckdb_destroy_logical_type`.
typedef struct _duckdb_logical_type {
	void *__lglt;
} * duckdb_logical_type;

//! Contains a data chunk from a duckdb_result.
//! Must be destroyed with `duckdb_destroy_data_chunk`.
typedef struct _duckdb_data_chunk {
	void *__dtck;
} * duckdb_data_chunk;

//! Holds a DuckDB value, which wraps a type.
//! Must be destroyed with `duckdb_destroy_value`.
typedef struct _duckdb_value {
	void *__val;
} * duckdb_value;

//===--------------------------------------------------------------------===//
// Function types
//===--------------------------------------------------------------------===//
//! Additional function info. When setting this info, it is necessary to pass a destroy-callback function.
typedef struct _duckdb_function_info {
	void *__val;
} * duckdb_function_info;

//===--------------------------------------------------------------------===//
// Scalar function types
//===--------------------------------------------------------------------===//
//! A scalar function. Must be destroyed with `duckdb_destroy_scalar_function`.
typedef struct _duckdb_scalar_function {
	void *__val;
} * duckdb_scalar_function;

//! The main function of the scalar function.
typedef void (*duckdb_scalar_function_t)(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output);

//===--------------------------------------------------------------------===//
// Table function types
//===--------------------------------------------------------------------===//

#ifndef DUCKDB_NO_EXTENSION_FUNCTIONS
//! A table function. Must be destroyed with `duckdb_destroy_table_function`.
typedef struct _duckdb_table_function {
	void *__val;
} * duckdb_table_function;

//! The bind info of the function. When setting this info, it is necessary to pass a destroy-callback function.
typedef struct _duckdb_bind_info {
	void *__val;
} * duckdb_bind_info;

//! Additional function init info. When setting this info, it is necessary to pass a destroy-callback function.
typedef struct _duckdb_init_info {
	void *__val;
} * duckdb_init_info;

//! The bind function of the table function.
typedef void (*duckdb_table_function_bind_t)(duckdb_bind_info info);

//! The (possibly thread-local) init function of the table function.
typedef void (*duckdb_table_function_init_t)(duckdb_init_info info);

//! The main function of the table function.
typedef void (*duckdb_table_function_t)(duckdb_function_info info, duckdb_data_chunk output);

//===--------------------------------------------------------------------===//
// Replacement scan types
//===--------------------------------------------------------------------===//

//! Additional replacement scan info. When setting this info, it is necessary to pass a destroy-callback function.
typedef struct _duckdb_replacement_scan_info {
	void *__val;
} * duckdb_replacement_scan_info;

//! A replacement scan function that can be added to a database.
typedef void (*duckdb_replacement_callback_t)(duckdb_replacement_scan_info info, const char *table_name, void *data);
#endif

//===--------------------------------------------------------------------===//
// Arrow-related types
//===--------------------------------------------------------------------===//

//! Holds an arrow query result. Must be destroyed with `duckdb_destroy_arrow`.
typedef struct _duckdb_arrow {
	void *__arrw;
} * duckdb_arrow;

//! Holds an arrow array stream. Must be destroyed with `duckdb_destroy_arrow_stream`.
typedef struct _duckdb_arrow_stream {
	void *__arrwstr;
} * duckdb_arrow_stream;

//! Holds an arrow schema. Remember to release the respective ArrowSchema object.
typedef struct _duckdb_arrow_schema {
	void *__arrs;
} * duckdb_arrow_schema;

//! Holds an arrow array. Remember to release the respective ArrowArray object.
typedef struct _duckdb_arrow_array {
	void *__arra;
} * duckdb_arrow_array;

//===--------------------------------------------------------------------===//
// Functions
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Open/Connect
//===--------------------------------------------------------------------===//

/*!
Creates a new database or opens an existing database file stored at the given path.
If no path is given a new in-memory database is created instead.
The instantiated database should be closed with 'duckdb_close'.

* path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
* out_database: The result database object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_open(const char *path, duckdb_database *out_database);

/*!
Extended version of duckdb_open. Creates a new database or opens an existing database file stored at the given path.
The instantiated database should be closed with 'duckdb_close'.

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
This should be called after you are done with any database allocated through `duckdb_open` or `duckdb_open_ext`.
Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
Still, it is recommended to always correctly close a database object after you are done with it.

* database: The database object to shut down.
*/
DUCKDB_API void duckdb_close(duckdb_database *database);

/*!
Opens a connection to a database. Connections are required to query the database, and store transactional state
associated with the connection.
The instantiated connection should be closed using 'duckdb_disconnect'.

* database: The database file to connect to.
* out_connection: The result connection object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);

/*!
Interrupt running query

* connection: The connection to interrupt
*/
DUCKDB_API void duckdb_interrupt(duckdb_connection connection);

/*!
Get progress of the running query

* connection: The working connection
* returns: -1 if no progress or a percentage of the progress
*/
DUCKDB_API duckdb_query_progress_type duckdb_query_progress(duckdb_connection connection);

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
The duckdb_config must be destroyed using 'duckdb_destroy_config'

This will always succeed unless there is a malloc failure.

Note that `duckdb_destroy_config` should always be called on the resulting config, even if the function returns
`DuckDBError`.

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
Destroys the specified configuration object and de-allocates all memory allocated for the object.

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
Returns the column name of the specified column. The result should not need to be freed; the column names will
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
Returns the statement type of the statement that was executed

* result: The result object to fetch the statement type from.
 * returns: duckdb_statement_type value or DUCKDB_STATEMENT_TYPE_INVALID
 */
DUCKDB_API duckdb_statement_type duckdb_result_statement_type(duckdb_result result);

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

#ifndef DUCKDB_API_NO_DEPRECATED
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Returns the number of rows present in the result object.

* result: The result object.
* returns: The number of rows present in the result object.
*/
DUCKDB_API idx_t duckdb_row_count(duckdb_result *result);
#endif

/*!
Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
queries. For other queries the rows_changed will be 0.

* result: The result object.
* returns: The number of rows changed.
*/
DUCKDB_API idx_t duckdb_rows_changed(duckdb_result *result);

#ifndef DUCKDB_API_NO_DEPRECATED
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
#endif

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
#ifndef DUCKDB_API_NO_DEPRECATED
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Fetches a data chunk from the duckdb_result. This function should be called repeatedly until the result is exhausted.

The result must be destroyed with `duckdb_destroy_data_chunk`.

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
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Checks if the type of the internal result is StreamQueryResult.

* result: The result object to check.
* returns: Whether or not the result object is of the type StreamQueryResult
*/
DUCKDB_API bool duckdb_result_is_streaming(duckdb_result result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Returns the number of data chunks present in the result.

* result: The result object
* returns: Number of data chunks present in the result.
*/
DUCKDB_API idx_t duckdb_result_chunk_count(duckdb_result result);
#endif

/*!
Returns the return_type of the given result, or DUCKDB_RETURN_TYPE_INVALID on error

* result: The result object
* returns: The return_type
 */
DUCKDB_API duckdb_result_type duckdb_result_return_type(duckdb_result result);

#ifndef DUCKDB_API_NO_DEPRECATED
//===--------------------------------------------------------------------===//
// Safe fetch functions
//===--------------------------------------------------------------------===//

// These functions will perform conversions if necessary.
// On failure (e.g. if conversion cannot be performed or if the value is NULL) a default value is returned.
// Note that these functions are slow since they perform bounds checking and conversion
// For fast access of values prefer using `duckdb_result_get_chunk`

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The boolean value at the specified location, or false if the value cannot be converted.
 */
DUCKDB_API bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The int8_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int8_t duckdb_value_int8(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The int16_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int16_t duckdb_value_int16(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The int32_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The int64_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_hugeint value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_hugeint duckdb_value_hugeint(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_uhugeint value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_uhugeint duckdb_value_uhugeint(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_decimal value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_decimal duckdb_value_decimal(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The uint8_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint8_t duckdb_value_uint8(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The uint16_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint16_t duckdb_value_uint16(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The uint32_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint32_t duckdb_value_uint32(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The uint64_t value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API uint64_t duckdb_value_uint64(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The float value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API float duckdb_value_float(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The double value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_date value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_time value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_timestamp value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The duckdb_interval value at the specified location, or 0 if the value cannot be converted.
 */
DUCKDB_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);

/*!
* DEPRECATED: use duckdb_value_string instead. This function does not work correctly if the string contains null bytes.
* returns: The text value at the specified location as a null-terminated string, or nullptr if the value cannot be
converted. The result must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: The string value at the specified location. Attempts to cast the result value to string.
 * No support for nested types, and for other complex types.
 * The resulting field "string.data" must be freed with `duckdb_free.`
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
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

* returns: The duckdb_blob value at the specified location. Returns a blob with blob.data set to nullptr if the
value cannot be converted. The resulting field "blob.data" must be freed with `duckdb_free.`
*/
DUCKDB_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 * returns: Returns true if the value at the specified index is NULL, and false otherwise.
 */
DUCKDB_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
#endif

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
Free a value returned from `duckdb_malloc`, `duckdb_value_varchar`, `duckdb_value_blob`, or
`duckdb_value_string`.

* ptr: The memory region to de-allocate.
*/
DUCKDB_API void duckdb_free(void *ptr);

/*!
The internal vector size used by DuckDB.
This is the amount of tuples that will fit into a data chunk created by `duckdb_create_data_chunk`.

* returns: The vector size.
*/
DUCKDB_API idx_t duckdb_vector_size();

/*!
Whether or not the duckdb_string_t value is inlined.
This means that the data of the string does not have a separate allocation.

*/
DUCKDB_API bool duckdb_string_is_inlined(duckdb_string_t string);

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
Test a `duckdb_date` to see if it is a finite value.

* date: The date object, as obtained from a `DUCKDB_TYPE_DATE` column.
* returns: True if the date is finite, false if it is ±infinity.
*/
DUCKDB_API bool duckdb_is_finite_date(duckdb_date date);

/*!
Decompose a `duckdb_time` object into hour, minute, second and microsecond (stored as `duckdb_time_struct`).

* time: The time object, as obtained from a `DUCKDB_TYPE_TIME` column.
* returns: The `duckdb_time_struct` with the decomposed elements.
*/
DUCKDB_API duckdb_time_struct duckdb_from_time(duckdb_time time);

/*!
Create a `duckdb_time_tz` object from micros and a timezone offset.

* micros: The microsecond component of the time.
* offset: The timezone offset component of the time.
* returns: The `duckdb_time_tz` element.
*/
DUCKDB_API duckdb_time_tz duckdb_create_time_tz(int64_t micros, int32_t offset);

/*!
Decompose a TIME_TZ objects into micros and a timezone offset.

Use `duckdb_from_time` to further decompose the micros into hour, minute, second and microsecond.

* micros: The time object, as obtained from a `DUCKDB_TYPE_TIME_TZ` column.
* out_micros: The microsecond component of the time.
* out_offset: The timezone offset component of the time.
*/
DUCKDB_API duckdb_time_tz_struct duckdb_from_time_tz(duckdb_time_tz micros);

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

/*!
Test a `duckdb_timestamp` to see if it is a finite value.

* ts: The timestamp object, as obtained from a `DUCKDB_TYPE_TIMESTAMP` column.
* returns: True if the timestamp is finite, false if it is ±infinity.
*/
DUCKDB_API bool duckdb_is_finite_timestamp(duckdb_timestamp ts);

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

//===--------------------------------------------------------------------===//
// Unsigned Hugeint Helpers
//===--------------------------------------------------------------------===//

/*!
Converts a duckdb_uhugeint object (as obtained from a `DUCKDB_TYPE_UHUGEINT` column) into a double.

* val: The uhugeint value.
* returns: The converted `double` element.
*/
DUCKDB_API double duckdb_uhugeint_to_double(duckdb_uhugeint val);

/*!
Converts a double value to a duckdb_uhugeint object.

If the conversion fails because the double value is too big the result will be 0.

* val: The double value.
* returns: The converted `duckdb_uhugeint` element.
*/
DUCKDB_API duckdb_uhugeint duckdb_double_to_uhugeint(double val);

//===--------------------------------------------------------------------===//
// Decimal Helpers
//===--------------------------------------------------------------------===//

/*!
Converts a double value to a duckdb_decimal object.

If the conversion fails because the double value is too big, or the width/scale are invalid the result will be 0.

* val: The double value.
* returns: The converted `duckdb_decimal` element.
*/
DUCKDB_API duckdb_decimal duckdb_double_to_decimal(double val, uint8_t width, uint8_t scale);

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
Returns the name used to identify the parameter
The returned string should be freed using `duckdb_free`.

Returns NULL if the index is out of range for the provided prepared statement.

* prepared_statement: The prepared statement for which to get the parameter name from.
*/
DUCKDB_API const char *duckdb_parameter_name(duckdb_prepared_statement prepared_statement, idx_t index);

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
Returns the statement type of the statement to be executed

 * statement: The prepared statement.
 * returns: duckdb_statement_type value or DUCKDB_STATEMENT_TYPE_INVALID
 */
DUCKDB_API duckdb_statement_type duckdb_prepared_statement_type(duckdb_prepared_statement statement);

//===--------------------------------------------------------------------===//
// Bind Values to Prepared Statements
//===--------------------------------------------------------------------===//

/*!
Binds a value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_value(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                          duckdb_value val);

/*!
Retrieve the index of the parameter for the prepared statement, identified by name
*/
DUCKDB_API duckdb_state duckdb_bind_parameter_index(duckdb_prepared_statement prepared_statement, idx_t *param_idx_out,
                                                    const char *name);

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
Binds a duckdb_hugeint value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            duckdb_hugeint val);
/*!
Binds an duckdb_uhugeint value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_uhugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                             duckdb_uhugeint val);
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
Binds a float value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);

/*!
Binds a double value to the prepared statement at the specified index.
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
Binds a duckdb_timestamp value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_timestamp_tz(duckdb_prepared_statement prepared_statement, idx_t param_idx,
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

//===--------------------------------------------------------------------===//
// Execute Prepared Statements
//===--------------------------------------------------------------------===//

/*!
Executes the prepared statement with the given bound parameters, and returns a materialized query result.

This method can be called multiple times for each prepared statement, and the parameters can be modified
between calls to this function.

Note that the result must be freed with `duckdb_destroy_result`.

* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
                                                duckdb_result *out_result);

#ifndef DUCKDB_API_NO_DEPRECATED
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Executes the prepared statement with the given bound parameters, and returns an optionally-streaming query result.
To determine if the resulting query was in fact streamed, use `duckdb_result_is_streaming`

This method can be called multiple times for each prepared statement, and the parameters can be modified
between calls to this function.

Note that the result must be freed with `duckdb_destroy_result`.

* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_prepared_streaming(duckdb_prepared_statement prepared_statement,
                                                          duckdb_result *out_result);
#endif

//===--------------------------------------------------------------------===//
// Extract Statements
//===--------------------------------------------------------------------===//

// A query string can be extracted into multiple SQL statements. Each statement can be prepared and executed separately.

/*!
Extract all statements from a query.
Note that after calling `duckdb_extract_statements`, the extracted statements should always be destroyed using
`duckdb_destroy_extracted`, even if no statements were extracted.

If the extract fails, `duckdb_extract_statements_error` can be called to obtain the reason why the extract failed.

* connection: The connection object
* query: The SQL query to extract
* out_extracted_statements: The resulting extracted statements object
* returns: The number of extracted statements or 0 on failure.
*/
DUCKDB_API idx_t duckdb_extract_statements(duckdb_connection connection, const char *query,
                                           duckdb_extracted_statements *out_extracted_statements);

/*!
Prepare an extracted statement.
Note that after calling `duckdb_prepare_extracted_statement`, the prepared statement should always be destroyed using
`duckdb_destroy_prepare`, even if the prepare fails.

If the prepare fails, `duckdb_prepare_error` can be called to obtain the reason why the prepare failed.

* connection: The connection object
* extracted_statements: The extracted statements object
* index: The index of the extracted statement to prepare
* out_prepared_statement: The resulting prepared statement object
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_prepare_extracted_statement(duckdb_connection connection,
                                                           duckdb_extracted_statements extracted_statements,
                                                           idx_t index,
                                                           duckdb_prepared_statement *out_prepared_statement);
/*!
Returns the error message contained within the extracted statements.
The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_extracted` is called.

* result: The extracted statements to fetch the error from.
* returns: The error of the extracted statements.
*/
DUCKDB_API const char *duckdb_extract_statements_error(duckdb_extracted_statements extracted_statements);

/*!
De-allocates all memory allocated for the extracted statements.
* extracted_statements: The extracted statements to destroy.
*/
DUCKDB_API void duckdb_destroy_extracted(duckdb_extracted_statements *extracted_statements);

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
#ifndef DUCKDB_API_NO_DEPRECATED
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Executes the prepared statement with the given bound parameters, and returns a pending result.
This pending result will create a streaming duckdb_result when executed.
The pending result represents an intermediate structure for a query that is not yet fully executed.

Note that after calling `duckdb_pending_prepared_streaming`, the pending result should always be destroyed using
`duckdb_destroy_pending`, even if this function returns DuckDBError.

* prepared_statement: The prepared statement to execute.
* out_result: The pending query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_pending_prepared_streaming(duckdb_prepared_statement prepared_statement,
                                                          duckdb_pending_result *out_result);
#endif

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

* pending_result: The pending result to execute a task within.
* returns: The state of the pending result after the execution.
*/
DUCKDB_API duckdb_pending_state duckdb_pending_execute_task(duckdb_pending_result pending_result);

/*!
If this returns DUCKDB_PENDING_RESULT_READY, the duckdb_execute_pending function can be called to obtain the result.
If this returns DUCKDB_PENDING_RESULT_NOT_READY, the duckdb_pending_execute_check_state function should be called again.
If this returns DUCKDB_PENDING_ERROR, an error occurred during execution.

The error message can be obtained by calling duckdb_pending_error on the pending_result.

* pending_result: The pending result.
* returns: The state of the pending result.
*/
DUCKDB_API duckdb_pending_state duckdb_pending_execute_check_state(duckdb_pending_result pending_result);

/*!
Fully execute a pending query result, returning the final query result.

If duckdb_pending_execute_task has been called until DUCKDB_PENDING_RESULT_READY was returned, this will return fast.
Otherwise, all remaining tasks must be executed first.

Note that the result must be freed with `duckdb_destroy_result`.

* pending_result: The pending result to execute.
* out_result: The result object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_pending(duckdb_pending_result pending_result, duckdb_result *out_result);

/*!
Returns whether a duckdb_pending_state is finished executing. For example if `pending_state` is
DUCKDB_PENDING_RESULT_READY, this function will return true.

* pending_state: The pending state on which to decide whether to finish execution.
* returns: Boolean indicating pending execution should be considered finished.
*/
DUCKDB_API bool duckdb_pending_execution_is_finished(duckdb_pending_state pending_state);

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
Creates a struct value from a type and an array of values

* type: The type of the struct
* values: The values for the struct fields
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_struct_value(duckdb_logical_type type, duckdb_value *values);

/*!
Creates a list value from a type and an array of values of length `value_count`

* type: The type of the list
* values: The values for the list
* value_count: The number of values in the list
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_list_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);

/*!
Creates an array value from a type and an array of values of length `value_count`

* type: The type of the array
* values: The values for the array
* value_count: The number of values in the array
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_create_array_value(duckdb_logical_type type, duckdb_value *values, idx_t value_count);

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
Returns the alias of a duckdb_logical_type, if one is set, else `NULL`.
The result must be destroyed with `duckdb_free`.

* type: The logical type to return the alias of
* returns: The alias or `NULL`
 */
DUCKDB_API char *duckdb_logical_type_get_alias(duckdb_logical_type type);

/*!
Creates a list type from its child type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* type: The child type of list type to create.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_list_type(duckdb_logical_type type);

/*!
Creates an array type from its child type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* type: The child type of array type to create.
* array_size: The number of elements in the array.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_array_type(duckdb_logical_type type, idx_t array_size);

/*!
Creates a map type from its key type and value type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* type: The key type and value type of map type to create.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_map_type(duckdb_logical_type key_type, duckdb_logical_type value_type);

/*!
Creates a UNION type from the passed types array.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* types: The array of types that the union should consist of.
* type_amount: The size of the types array.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_union_type(duckdb_logical_type *member_types, const char **member_names,
                                                        idx_t member_count);

/*!
Creates a STRUCT type from the passed member name and type arrays.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* member_types: The array of types that the struct should consist of.
* member_names: The array of names that the struct should consist of.
* member_count: The number of members that were specified for both arrays.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_struct_type(duckdb_logical_type *member_types, const char **member_names,
                                                         idx_t member_count);

/*!
Creates an ENUM type from the passed member name array.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* enum_name: The name of the enum.
* member_names: The array of names that the enum should consist of.
* member_count: The number of elements that were specified in the array.
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_enum_type(const char **member_names, idx_t member_count);

/*!
Creates a `duckdb_logical_type` of type decimal with the specified width and scale.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* width: The width of the decimal type
* scale: The scale of the decimal type
* returns: The logical type.
*/
DUCKDB_API duckdb_logical_type duckdb_create_decimal_type(uint8_t width, uint8_t scale);

/*!
Retrieves the enum type class of a `duckdb_logical_type`.

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
Retrieves the dictionary size of the enum type.

* type: The logical type object
* returns: The dictionary size of the enum type
*/
DUCKDB_API uint32_t duckdb_enum_dictionary_size(duckdb_logical_type type);

/*!
Retrieves the dictionary value at the specified position from the enum.

The result must be freed with `duckdb_free`.

* type: The logical type object
* index: The index in the dictionary
* returns: The string value of the enum type. Must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index);

/*!
Retrieves the child type of the given list type.

The result must be freed with `duckdb_destroy_logical_type`.

* type: The logical type object
* returns: The child type of the list type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);

/*!
Retrieves the child type of the given array type.

The result must be freed with `duckdb_destroy_logical_type`.

* type: The logical type object
* returns: The child type of the array type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type);

/*!
Retrieves the array size of the given array type.

* type: The logical type object
* returns: The fixed number of elements the values of this array type can store.
*/
DUCKDB_API idx_t duckdb_array_type_array_size(duckdb_logical_type type);

/*!
Retrieves the key type of the given map type.

The result must be freed with `duckdb_destroy_logical_type`.

* type: The logical type object
* returns: The key type of the map type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type);

/*!
Retrieves the value type of the given map type.

The result must be freed with `duckdb_destroy_logical_type`.

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

The result must be freed with `duckdb_free`.

* type: The logical type object
* index: The child index
* returns: The name of the struct type. Must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index);

/*!
Retrieves the child type of the given struct type at the specified index.

The result must be freed with `duckdb_destroy_logical_type`.

* type: The logical type object
* index: The child index
* returns: The child type of the struct type. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index);

/*!
Returns the number of members that the union type has.

* type: The logical type (union) object
* returns: The number of members of a union type.
*/
DUCKDB_API idx_t duckdb_union_type_member_count(duckdb_logical_type type);

/*!
Retrieves the name of the union member.

The result must be freed with `duckdb_free`.

* type: The logical type object
* index: The child index
* returns: The name of the union member. Must be freed with `duckdb_free`.
*/
DUCKDB_API char *duckdb_union_type_member_name(duckdb_logical_type type, idx_t index);

/*!
Retrieves the child type of the given union member at the specified index.

The result must be freed with `duckdb_destroy_logical_type`.

* type: The logical type object
* index: The child index
* returns: The child type of the union member. Must be destroyed with `duckdb_destroy_logical_type`.
*/
DUCKDB_API duckdb_logical_type duckdb_union_type_member_type(duckdb_logical_type type, idx_t index);

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

Note that the result must be destroyed with `duckdb_destroy_data_chunk`.

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
Assigns a string element in the vector at the specified location. You may also use this function to assign BLOBs.

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
Returns the size of the child vector of the list.

* vector: The vector
* returns: The size of the child list
*/
DUCKDB_API idx_t duckdb_list_vector_get_size(duckdb_vector vector);

/*!
Sets the total size of the underlying child-vector of a list vector.

* vector: The list vector.
* size: The size of the child list.
* returns: The duckdb state. Returns DuckDBError if the vector is nullptr.
*/
DUCKDB_API duckdb_state duckdb_list_vector_set_size(duckdb_vector vector, idx_t size);

/*!
Sets the total capacity of the underlying child-vector of a list.

* vector: The list vector.
* required_capacity: the total capacity to reserve.
* return: The duckdb state. Returns DuckDBError if the vector is nullptr.
*/
DUCKDB_API duckdb_state duckdb_list_vector_reserve(duckdb_vector vector, idx_t required_capacity);

/*!
Retrieves the child vector of a struct vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* index: The child index
* returns: The child vector
*/
DUCKDB_API duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index);

/*!
Retrieves the child vector of a array vector.

The resulting vector is valid as long as the parent vector is valid.
The resulting vector has the size of the parent vector multiplied by the array size.

* vector: The vector
* returns: The child vector
*/
DUCKDB_API duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector);

//===--------------------------------------------------------------------===//
// Validity Mask Functions
//===--------------------------------------------------------------------===//

/*!
Returns whether or not a row is valid (i.e. not NULL) in the given validity mask.

* validity: The validity mask, as obtained through `duckdb_vector_get_validity`
* row: The row index
* returns: true if the row is valid, false otherwise
*/
DUCKDB_API bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row);

/*!
In a validity mask, sets a specific row to either valid or invalid.

Note that `duckdb_vector_ensure_validity_writable` should be called before calling `duckdb_vector_get_validity`,
to ensure that there is a validity mask to write to.

* validity: The validity mask, as obtained through `duckdb_vector_get_validity`.
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

#ifndef DUCKDB_NO_EXTENSION_FUNCTIONS
//===--------------------------------------------------------------------===//
// Scalar Functions
//===--------------------------------------------------------------------===//
/*!
Creates a new empty scalar function.

The return value should be destroyed with `duckdb_destroy_scalar_function`.

* returns: The scalar function object.
*/
DUCKDB_API duckdb_scalar_function duckdb_create_scalar_function();

/*!
Destroys the given scalar function object.

* scalar_function: The scalar function to destroy
*/
DUCKDB_API void duckdb_destroy_scalar_function(duckdb_scalar_function *scalar_function);

/*!
Sets the name of the given scalar function.

* scalar_function: The scalar function
* name: The name of the scalar function
*/
DUCKDB_API void duckdb_scalar_function_set_name(duckdb_scalar_function scalar_function, const char *name);

/*!
Sets the parameters of the given scalar function to varargs. Does not require adding parameters with
duckdb_scalar_function_add_parameter.

* scalar_function: The scalar function
* type: The type of the arguments
*/
DUCKDB_API void duckdb_scalar_function_set_varargs(duckdb_scalar_function scalar_function, duckdb_logical_type type);

/*!
Adds a parameter to the scalar function.

* scalar_function: The scalar function
* type: The type of the parameter to add.
*/
DUCKDB_API void duckdb_scalar_function_add_parameter(duckdb_scalar_function scalar_function, duckdb_logical_type type);

/*!
Sets the return type of the scalar function.

* scalar_function: The scalar function
* type: The return type to set
*/
DUCKDB_API void duckdb_scalar_function_set_return_type(duckdb_scalar_function scalar_function,
                                                       duckdb_logical_type type);

/*!
Assigns extra information to the scalar function that can be fetched during binding, etc.

* scalar_function: The scalar function
* extra_info: The extra information
* destroy: The callback that will be called to destroy the bind data (if any)
*/
DUCKDB_API void duckdb_scalar_function_set_extra_info(duckdb_scalar_function scalar_function, void *extra_info,
                                                      duckdb_delete_callback_t destroy);

/*!
Sets the main function of the scalar function.

* scalar_function: The scalar function
* function: The function
*/
DUCKDB_API void duckdb_scalar_function_set_function(duckdb_scalar_function scalar_function,
                                                    duckdb_scalar_function_t function);

/*!
Register the scalar function object within the given connection.

The function requires at least a name, a function and a return type.

If the function is incomplete or a function with this name already exists DuckDBError is returned.

* con: The connection to register it in.
* function: The function pointer
* returns: Whether or not the registration was successful.
*/
DUCKDB_API duckdb_state duckdb_register_scalar_function(duckdb_connection con, duckdb_scalar_function scalar_function);

/*!
Retrieves the extra info of the function as set in `duckdb_scalar_function_set_extra_info`.

* info: The info object
* returns: The extra info
*/
DUCKDB_API void *duckdb_scalar_function_get_extra_info(duckdb_function_info info);

/*!
Report that an error has occurred while executing the scalar function.

* info: The info object
* error: The error message
*/
DUCKDB_API void duckdb_scalar_function_set_error(duckdb_function_info info, const char *error);

//===--------------------------------------------------------------------===//
// Table Functions
//===--------------------------------------------------------------------===//

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
Adds a named parameter to the table function.

* table_function: The table function
* name: The name of the parameter
* type: The type of the parameter to add.
*/
DUCKDB_API void duckdb_table_function_add_named_parameter(duckdb_table_function table_function, const char *name,
                                                          duckdb_logical_type type);

/*!
Assigns extra information to the table function that can be fetched during binding, etc.

* table_function: The table function
* extra_info: The extra information
* destroy: The callback that will be called to destroy the bind data (if any)
*/
DUCKDB_API void duckdb_table_function_set_extra_info(duckdb_table_function table_function, void *extra_info,
                                                     duckdb_delete_callback_t destroy);

/*!
Sets the bind function of the table function.

* table_function: The table function
* bind: The bind function
*/
DUCKDB_API void duckdb_table_function_set_bind(duckdb_table_function table_function, duckdb_table_function_bind_t bind);

/*!
Sets the init function of the table function.

* table_function: The table function
* init: The init function
*/
DUCKDB_API void duckdb_table_function_set_init(duckdb_table_function table_function, duckdb_table_function_init_t init);

/*!
Sets the thread-local init function of the table function.

* table_function: The table function
* init: The init function
*/
DUCKDB_API void duckdb_table_function_set_local_init(duckdb_table_function table_function,
                                                     duckdb_table_function_init_t init);

/*!
Sets the main function of the table function.

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
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`.

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
Retrieves a named parameter with the given name.

The result must be destroyed with `duckdb_destroy_value`.

* info: The info object
* name: The name of the parameter
* returns: The value of the parameter. Must be destroyed with `duckdb_destroy_value`.
*/
DUCKDB_API duckdb_value duckdb_bind_get_named_parameter(duckdb_bind_info info, const char *name);

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
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`.

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
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`.

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

/*!
Add a replacement scan definition to the specified database.

* db: The database object to add the replacement scan to
* replacement: The replacement scan callback
* extra_data: Extra data that is passed back into the specified callback
* delete_callback: The delete callback to call on the extra data, if any
*/
DUCKDB_API void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement,
                                            void *extra_data, duckdb_delete_callback_t delete_callback);

/*!
Sets the replacement function name. If this function is called in the replacement callback,
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
#endif

//===--------------------------------------------------------------------===//
// Appender
//===--------------------------------------------------------------------===//

// Appenders are the most efficient way of loading data into DuckDB from within the C interface, and are recommended for
// fast data loading. The appender is much faster than using prepared statements or individual `INSERT INTO` statements.

// Appends are made in row-wise format. For every column, a `duckdb_append_[type]` call should be made, after which
// the row should be finished by calling `duckdb_appender_end_row`. After all rows have been appended,
// `duckdb_appender_destroy` should be used to finalize the appender and clean up the resulting memory.

// Instead of appending rows with `duckdb_appender_end_row`, it is also possible to fill and append
// chunks-at-a-time.

// Note that `duckdb_appender_destroy` should always be called on the resulting appender, even if the function returns
// `DuckDBError`.

/*!
Creates an appender object.

Note that the object must be destroyed with `duckdb_appender_destroy`.

* connection: The connection context to create the appender in.
* schema: The schema of the table to append to, or `nullptr` for the default schema.
* table: The table name to append to.
* out_appender: The resulting appender object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                               duckdb_appender *out_appender);

/*!
Returns the number of columns in the table that belongs to the appender.

* appender The appender to get the column count from.
* returns: The number of columns in the table.
*/
DUCKDB_API idx_t duckdb_appender_column_count(duckdb_appender appender);

/*!
Returns the type of the column at the specified index.

Note: The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* appender The appender to get the column type from.
* col_idx The index of the column to get the type of.
* returns: The duckdb_logical_type of the column.
*/
DUCKDB_API duckdb_logical_type duckdb_appender_column_type(duckdb_appender appender, idx_t col_idx);

/*!
Returns the error message associated with the given appender.
If the appender has no error message, this returns `nullptr` instead.

The error message should not be freed. It will be de-allocated when `duckdb_appender_destroy` is called.

* appender: The appender to get the error from.
* returns: The error message, or `nullptr` if there is none.
*/
DUCKDB_API const char *duckdb_appender_error(duckdb_appender appender);

/*!
Flush the appender to the table, forcing the cache of the appender to be cleared. If flushing the data triggers a
constraint violation or any other error, then all data is invalidated, and this function returns DuckDBError.
It is not possible to append more values. Call duckdb_appender_error to obtain the error message followed by
duckdb_appender_destroy to destroy the invalidated appender.

* appender: The appender to flush.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_flush(duckdb_appender appender);

/*!
Closes the appender by flushing all intermediate states and closing it for further appends. If flushing the data
triggers a constraint violation or any other error, then all data is invalidated, and this function returns DuckDBError.
Call duckdb_appender_error to obtain the error message followed by duckdb_appender_destroy to destroy the invalidated
appender.

* appender: The appender to flush and close.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_appender_close(duckdb_appender appender);

/*!
Closes the appender by flushing all intermediate states to the table and destroying it. By destroying it, this function
de-allocates all memory associated with the appender. If flushing the data triggers a constraint violation,
then all data is invalidated, and this function returns DuckDBError. Due to the destruction of the appender, it is no
longer possible to obtain the specific error message with duckdb_appender_error. Therefore, call duckdb_appender_close
before destroying the appender, if you need insights into the specific error.

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
Append a DEFAULT value (NULL if DEFAULT not available for column) to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_default(duckdb_appender appender);

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
Append a duckdb_uhugeint value to the appender.
*/
DUCKDB_API duckdb_state duckdb_append_uhugeint(duckdb_appender appender, duckdb_uhugeint value);

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
// TableDescription
//===--------------------------------------------------------------------===//

/*!
Creates a table description object.
Note that `duckdb_table_description_destroy` should always be called on the resulting table_description, even if the
function returns `DuckDBError`.
* connection: The connection context.
* schema: The schema of the table, or `nullptr` for the default schema.
* table: The table name.
* out: The resulting table description object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_table_description_create(duckdb_connection connection, const char *schema,
                                                        const char *table, duckdb_table_description *out);

/*!
Destroy the TableDescription object.
* table: The table_description to destroy.
*/
DUCKDB_API void duckdb_table_description_destroy(duckdb_table_description *table_description);

/*!
Returns the error message associated with the given table_description.
If the table_description has no error message, this returns `nullptr` instead.
The error message should not be freed. It will be de-allocated when `duckdb_table_description_destroy` is called.
* table_description: The table_description to get the error from.
* returns: The error message, or `nullptr` if there is none.
*/
DUCKDB_API const char *duckdb_table_description_error(duckdb_table_description table);

/*!
Check if the column at 'index' index of the table has a DEFAULT expression.
* table: The table_description to query.
* index: The index of the column to query.
* out: The out-parameter used to store the result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_column_has_default(duckdb_table_description table_description, idx_t index, bool *out);

#ifndef DUCKDB_API_NO_DEPRECATED
//===--------------------------------------------------------------------===//
// Arrow Interface
//===--------------------------------------------------------------------===//

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

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
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Fetch the internal arrow schema from the arrow result. Remember to call release on the respective
ArrowSchema object.

* result: The result to fetch the schema from.
* out_schema: The output schema.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Fetch the internal arrow schema from the prepared statement. Remember to call release on the respective
ArrowSchema object.

* result: The prepared statement to fetch the schema from.
* out_schema: The output schema.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_prepared_arrow_schema(duckdb_prepared_statement prepared,
                                                     duckdb_arrow_schema *out_schema);
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Convert a data chunk into an arrow struct array. Remember to call release on the respective
ArrowArray object.

* result: The result object the data chunk have been fetched from.
* chunk: The data chunk to convert.
* out_array: The output array.
*/
DUCKDB_API void duckdb_result_arrow_array(duckdb_result result, duckdb_data_chunk chunk, duckdb_arrow_array *out_array);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Fetch an internal arrow struct array from the arrow result. Remember to call release on the respective
ArrowArray object.

This function can be called multiple time to get next chunks, which will free the previous out_array.
So consume the out_array before calling this function again.

* result: The result to fetch the array from.
* out_array: The output array.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Returns the number of columns present in the arrow result object.

* result: The result object.
* returns: The number of columns present in the result object.
*/
DUCKDB_API idx_t duckdb_arrow_column_count(duckdb_arrow result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Returns the number of rows present in the arrow result object.

* result: The result object.
* returns: The number of rows present in the result object.
*/
DUCKDB_API idx_t duckdb_arrow_row_count(duckdb_arrow result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Returns the number of rows changed by the query stored in the arrow result. This is relevant only for
INSERT/UPDATE/DELETE queries. For other queries the rows_changed will be 0.

* result: The result object.
* returns: The number of rows changed.
*/
DUCKDB_API idx_t duckdb_arrow_rows_changed(duckdb_arrow result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

 Returns the error message contained within the result. The error is only set if `duckdb_query_arrow` returns
`DuckDBError`.

The error message should not be freed. It will be de-allocated when `duckdb_destroy_arrow` is called.

* result: The result object to fetch the error from.
* returns: The error of the result.
*/
DUCKDB_API const char *duckdb_query_arrow_error(duckdb_arrow result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Closes the result and de-allocates all memory allocated for the arrow result.

* result: The result to destroy.
*/
DUCKDB_API void duckdb_destroy_arrow(duckdb_arrow *result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Releases the arrow array stream and de-allocates its memory.

* stream: The arrow array stream to destroy.
*/
DUCKDB_API void duckdb_destroy_arrow_stream(duckdb_arrow_stream *stream_p);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Executes the prepared statement with the given bound parameters, and returns an arrow query result.
Note that after running `duckdb_execute_prepared_arrow`, `duckdb_destroy_arrow` must be called on the result object.

* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement,
                                                      duckdb_arrow *out_result);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Scans the Arrow stream and creates a view with the given name.

* connection: The connection on which to execute the scan.
* table_name: Name of the temporary view to create.
* arrow: Arrow stream wrapper.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_arrow_scan(duckdb_connection connection, const char *table_name,
                                          duckdb_arrow_stream arrow);

/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Scans the Arrow array and creates a view with the given name.
Note that after running `duckdb_arrow_array_scan`, `duckdb_destroy_arrow_stream` must be called on the out stream.

* connection: The connection on which to execute the scan.
* table_name: Name of the temporary view to create.
* arrow_schema: Arrow schema wrapper.
* arrow_array: Arrow array wrapper.
* out_stream: Output array stream that wraps around the passed schema, for releasing/deleting once done.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
*/
DUCKDB_API duckdb_state duckdb_arrow_array_scan(duckdb_connection connection, const char *table_name,
                                                duckdb_arrow_schema arrow_schema, duckdb_arrow_array arrow_array,
                                                duckdb_arrow_stream *out_stream);
#endif

#ifndef DUCKDB_NO_EXTENSION_FUNCTIONS
//===--------------------------------------------------------------------===//
// Threading Information
//===--------------------------------------------------------------------===//

/*!
Execute DuckDB tasks on this thread.

Will return after `max_tasks` have been executed, or if there are no more tasks present.

* database: The database object to execute tasks for
* max_tasks: The maximum amount of tasks to execute
*/
DUCKDB_API void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks);

/*!
Creates a task state that can be used with duckdb_execute_tasks_state to execute tasks until
`duckdb_finish_execution` is called on the state.

`duckdb_destroy_state` must be called on the result.

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

/*!
Returns true if the execution of the current query is finished.

* con: The connection on which to check
*/
DUCKDB_API bool duckdb_execution_is_finished(duckdb_connection con);
#endif

//===--------------------------------------------------------------------===//
// Streaming Result Interface
//===--------------------------------------------------------------------===//
#ifndef DUCKDB_API_NO_DEPRECATED
/*!
**DEPRECATION NOTICE**: This method is scheduled for removal in a future release.

Fetches a data chunk from the (streaming) duckdb_result. This function should be called repeatedly until the result is
exhausted.

The result must be destroyed with `duckdb_destroy_data_chunk`.

This function can only be used on duckdb_results created with 'duckdb_pending_prepared_streaming'

If this function is used, none of the other result functions can be used and vice versa (i.e. this function cannot be
mixed with the legacy result functions or the materialized result functions).

It is not known beforehand how many chunks will be returned by this result.

* result: The result object to fetch the data chunk from.
* returns: The resulting data chunk. Returns `NULL` if the result has an error.
*/
DUCKDB_API duckdb_data_chunk duckdb_stream_fetch_chunk(duckdb_result result);
#endif

/*!
Fetches a data chunk from a duckdb_result. This function should be called repeatedly until the result is exhausted.

The result must be destroyed with `duckdb_destroy_data_chunk`.

It is not known beforehand how many chunks will be returned by this result.

* result: The result object to fetch the data chunk from.
* returns: The resulting data chunk. Returns `NULL` if the result has an error.
*/
DUCKDB_API duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result);

#ifdef __cplusplus
}
#endif
