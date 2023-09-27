using Base.Libc

if "JULIA_DUCKDB_LIBRARY" in keys(ENV)
    libduckdb = ENV["JULIA_DUCKDB_LIBRARY"]
else
    using DuckDB_jll
end

#=//===--------------------------------------------------------------------===//
// Open/Connect
//===--------------------------------------------------------------------===//
=#
"""
	duckdb_open(path, out_database)
Creates a new database or opens an existing database file stored at the the given path.
If no path is given a new in-memory database is created instead.
* `path`: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
* `out_database`: The result database object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_open(path, out_database)
    return ccall((:duckdb_open, libduckdb), duckdb_state, (Ptr{UInt8}, Ref{duckdb_database}), path, out_database)
end
"""
	Extended version of duckdb_open. Creates a new database or opens an existing database file stored at the the given path.

    * path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
    * out_database: The result database object.
    * config: (Optional) configuration used to start up the database system.
    * out_error: If set and the function returns DuckDBError, this will contain the reason why the start-up failed.
    Note that the error must be freed using `duckdb_free`.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_open_ext(path, out_database, config, out_error)
    return ccall(
        (:duckdb_open_ext, libduckdb),
        duckdb_state,
        (Ptr{UInt8}, Ref{duckdb_database}, duckdb_config, Ptr{Ptr{UInt8}}),
        path,
        out_database,
        config,
        out_error
    )
end
"""
	duckdb_close(database)
Closes the specified database and de-allocates all memory allocated for that database.
This should be called after you are done with any database allocated through `duckdb_open`.
Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
Still it is recommended to always correctly close a database object after you are done with it.
* `database`: The database object to shut down.
"""
function duckdb_close(database)
    return ccall((:duckdb_close, libduckdb), Cvoid, (Ref{duckdb_database},), database)
end
"""
	duckdb_connect(database, out_connection)
Opens a connection to a database. Connections are required to query the database, and store transactional state
associated with the connection.
* `database`: The database file to connect to.
* `out_connection`: The result connection object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_connect(database, out_connection)
    return ccall(
        (:duckdb_connect, libduckdb),
        duckdb_state,
        (duckdb_database, Ref{duckdb_connection}),
        database,
        out_connection
    )
end

"""
	duckdb_disconnect(connection)
Closes the specified connection and de-allocates all memory allocated for that connection.
* `connection`: The connection to close.
"""
function duckdb_disconnect(connection)
    return ccall((:duckdb_disconnect, libduckdb), Cvoid, (Ref{duckdb_connection},), connection)
end

#=
//===--------------------------------------------------------------------===//
// Configuration
//===--------------------------------------------------------------------===//
=#

"""
	duckdb_create_config(config)
Initializes an empty configuration object that can be used to provide start-up options for the DuckDB instance
through `duckdb_open_ext`.
This will always succeed unless there is a malloc failure.
* `out_config`: The result configuration object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_create_config(config)
    return ccall((:duckdb_create_config, libduckdb), duckdb_state, (Ref{duckdb_config},), config)
end

"""
	duckdb_config_count()
This returns the total amount of configuration options available for usage with `duckdb_get_config_flag`.
This should not be called in a loop as it internally loops over all the options.
* returns: The amount of config options available.
"""
function duckdb_config_count()
    return ccall((:duckdb_config_count, libduckdb), Int32, ())
end

"""
	duckdb_get_config_flag(index,out_name,out_description)
Obtains a human-readable name and description of a specific configuration option. This can be used to e.g.
display configuration options. This will succeed unless `index` is out of range (i.e. `>= duckdb_config_count`).
The result name or description MUST NOT be freed.
* `index`: The index of the configuration option (between 0 and `duckdb_config_count`)
* `out_name`: A name of the configuration flag.
* `out_description`: A description of the configuration flag.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_get_config_flag(index, out_name, out_description)
    return ccall(
        (:duckdb_get_config_flag, libduckdb),
        duckdb_state,
        (Int32, Ptr{Ptr{UInt8}}, Ptr{Ptr{UInt8}}),
        index,
        out_name,
        out_description
    )
end

"""
	duckdb_set_config(config,name,option)
Sets the specified option for the specified configuration. The configuration option is indicated by name.
To obtain a list of config options, see `duckdb_get_config_flag`.
In the source code, configuration options are defined in `config.cpp`.
This can fail if either the name is invalid, or if the value provided for the option is invalid.
* `duckdb_config`: The configuration object to set the option on.
* `name`: The name of the configuration flag to set.
* `option`: The value to set the configuration flag to.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_set_config(config, name, option)
    return ccall(
        (:duckdb_set_config, libduckdb),
        duckdb_state,
        (duckdb_config, Ptr{UInt8}, Ptr{UInt8}),
        config,
        name,
        option
    )
end

"""
	duckdb_destroy_config(config)
Destroys the specified configuration option and de-allocates all memory allocated for the object.
* `config`: The configuration object to destroy.
"""
function duckdb_destroy_config(config)
    return ccall((:duckdb_destroy_config, libduckdb), Cvoid, (Ref{duckdb_config},), config)
end

# #=
# //===--------------------------------------------------------------------===//
# // Query Execution
# //===--------------------------------------------------------------------===//
# =#
#
# """
# 	duckdb_query(connection,query,out_result)
# Executes a SQL query within a connection and stores the full (materialized) result in the out_result pointer.
# If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
# `duckdb_result_error`.
# Note that after running `duckdb_query`, `duckdb_destroy_result` must be called on the result object even if the
# query fails, otherwise the error stored within the result will not be freed correctly.
# * `connection`: The connection to perform the query in.
# * `query`: The SQL query to run.
# * `out_result`: The query result.
# * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
# """
# function duckdb_query(connection, query, out_result)
#     return ccall(
#         (:duckdb_query, libduckdb),
#         Int32,
#         (Ptr{Cvoid}, Ptr{UInt8}, Ptr{Cvoid}),
#         connection[],
#         query,
#         out_result,
#     )
# end
#
"""
	duckdb_destroy_result(result)
Closes the result and de-allocates all memory allocated for that connection.
* `result`: The result to destroy.
"""
function duckdb_destroy_result(result)
    return ccall((:duckdb_destroy_result, libduckdb), Cvoid, (Ref{duckdb_result},), result)
end

"""
	duckdb_column_name(result,col)
Returns the column name of the specified column. The result should not need be freed; the column names will
automatically be destroyed when the result is destroyed.
Returns `NULL` if the column is out of range.
* `result`: The result object to fetch the column name from.
* `col`: The column index.
* returns: The column name of the specified column.
"""
function duckdb_column_name(result, col)
    return ccall((:duckdb_column_name, libduckdb), Ptr{UInt8}, (Ref{duckdb_result}, Int32), result, col - 1)
end

"""
	duckdb_column_type(result,col)
Returns the column type of the specified column.
Returns `DUCKDB_TYPE_INVALID` if the column is out of range.
* `result`: The result object to fetch the column type from.
* `col`: The column index.
* returns: The column type of the specified column.
"""
function duckdb_column_type(result, col)
    return ccall((:duckdb_column_type, libduckdb), DUCKDB_TYPE, (Ref{duckdb_result}, Int32), result, col - 1)
end

"""
Returns the logical column type of the specified column.

The return type of this call should be destroyed with `duckdb_destroy_logical_type`.

Returns `NULL` if the column is out of range.

* result: The result object to fetch the column type from.
* col: The column index.
* returns: The logical column type of the specified column.
"""
function duckdb_column_logical_type(result, col)
    return ccall(
        (:duckdb_column_logical_type, libduckdb),
        duckdb_logical_type,
        (Ref{duckdb_result}, Int32),
        result,
        col - 1
    )
end

"""
	duckdb_column_count(result)
Returns the number of columns present in a the result object.
* `result`: The result object.
* returns: The number of columns present in the result object.
"""
function duckdb_column_count(result)
    return ccall((:duckdb_column_count, libduckdb), Int32, (Ref{duckdb_result},), result)
end

"""
	duckdb_row_count(result)
Returns the number of rows present in a the result object.
* `result`: The result object.
* returns: The number of rows present in the result object.
"""
function duckdb_row_count(result)
    return ccall((:duckdb_row_count, libduckdb), Int64, (Ref{duckdb_result},), result)
end

"""
	duckdb_rows_changed(result)
Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
queries. For other queries the rows_changed will be 0.
* `result`: The result object.
* returns: The number of rows changed.
"""
function duckdb_rows_changed(result)
    return ccall((:duckdb_rows_changed, libduckdb), Int64, (Ptr{Cvoid},), result)
end

"""
	duckdb_column_data(result,col)
Returns the data of a specific column of a result in columnar format. This is the fastest way of accessing data in a
query result, as no conversion or type checking must be performed (outside of the original switch). If performance
is a concern, it is recommended to use this API over the `duckdb_value` functions.
The function returns a dense array which contains the result data. The exact type stored in the array depends on the
corresponding duckdb_type (as provided by `duckdb_column_type`). For the exact type by which the data should be
accessed, see the comments in [the types section](types) or the `DUCKDB_TYPE` enum.
For example, for a column of type `DUCKDB_TYPE_INTEGER`, rows can be accessed in the following manner:
```c
int32_t *data = (int32_t *) duckdb_column_data(&result, 0);
printf("Data for row %d: %d\\n", row, data[row]);
```
* `result`: The result object to fetch the column data from.
* `col`: The column index.
* returns: The column data of the specified column.
"""
function duckdb_column_data(result, col)
    return ccall((:duckdb_column_data, libduckdb), Ptr{Cvoid}, (Ref{duckdb_result}, Int32), result, col - 1)
end

"""
	duckdb_nullmask_data(result,col)
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
* `result`: The result object to fetch the nullmask from.
* `col`: The column index.
* returns: The nullmask of the specified column.
"""
function duckdb_nullmask_data(result, col)
    return ccall((:duckdb_nullmask_data, libduckdb), Ptr{Int32}, (Ref{duckdb_result}, Int32), result, col - 1)
end

"""
	duckdb_result_error(result)
Returns the error message contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.
The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_result` is called.
* `result`: The result object to fetch the nullmask from.
* returns: The error of the result.
"""
function duckdb_result_error(result)
    return ccall((:duckdb_result_error, libduckdb), Ptr{UInt8}, (Ref{duckdb_result},), result)
end

#=
//===--------------------------------------------------------------------===//
// Result Functions
//===--------------------------------------------------------------------===//
=#

"""
Fetches a data chunk from the duckdb_result. This function should be called repeatedly until the result is exhausted.

This function supersedes all `duckdb_value` functions, as well as the `duckdb_column_data` and `duckdb_nullmask_data` functions.
It results in significantly better performance, and should be preferred in newer code-bases.

If this function is used, none of the other result functions can be used and vice versa (i.e. this function cannot be mixed with the legacy result functions).

Use `duckdb_result_chunk_count` to figure out how many chunks there are in the result.

* result: The result object to fetch the data chunk from.
* chunk_index: The chunk index to fetch from.
* returns: The resulting data chunk. Returns `NULL` if the chunk index is out of bounds.
"""
function duckdb_result_get_chunk(result, chunk_index)
    return ccall(
        (:duckdb_result_get_chunk, libduckdb),
        duckdb_data_chunk,
        (duckdb_result, UInt64),
        result,
        chunk_index - 1
    )
end

"""
 Checks if the type of the internal result is StreamQueryResult.

 * result: The result object to check.
 * returns: Whether or not the result object is of the type StreamQueryResult
"""
function duckdb_result_is_streaming(result)
    return ccall((:duckdb_result_is_streaming, libduckdb), Bool, (duckdb_result,), result)
end

"""
 Fetches a data chunk from the (streaming) duckdb_result. This function should be called repeatedly until the result is
 exhausted.

 The result must be destroyed with `duckdb_destroy_data_chunk`.

 This function can only be used on duckdb_results created with 'duckdb_pending_prepared_streaming'

 If this function is used, none of the other result functions can be used and vice versa (i.e. this function cannot be
 mixed with the legacy result functions or the materialized result functions).

 It is not known beforehand how many chunks will be returned by this result.

 * result: The result object to fetch the data chunk from.
 * returns: The resulting data chunk. Returns `NULL` if the result has an error.
"""
function duckdb_stream_fetch_chunk(result)
    return ccall((:duckdb_stream_fetch_chunk, libduckdb), duckdb_data_chunk, (duckdb_result,), result)
end




"""
Returns the number of data chunks present in the result.

* result: The result object
* returns: The resulting data chunk. Returns `NULL` if the chunk index is out of bounds.
"""
function duckdb_result_chunk_count(result)
    return ccall((:duckdb_result_chunk_count, libduckdb), UInt64, (duckdb_result,), result)
end


"""
	duckdb_value_boolean(result,col,row)
* returns: The boolean value at the specified location, or false if the value cannot be converted.
"""
function duckdb_value_boolean(result, col, row)
    return ccall(
        (:duckdb_value_boolean, libduckdb),
        Int32,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_int8(result,col,row)
* returns: The int8_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_int8(result, col, row)
    return ccall((:duckdb_value_int8, libduckdb), Int8, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
	duckdb_value_int16(result,col,row)
 * returns: The int16_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_int16(result, col, row)
    return ccall((:duckdb_value_int16, libduckdb), Int16, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
	duckdb_value_int32(result,col,row)
 * returns: The int32_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_int32(result, col, row)
    return ccall((:duckdb_value_int32, libduckdb), Int32, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
	duckdb_value_int64(result,col,row)
 * returns: The int64_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_int64(result, col, row)
    return ccall((:duckdb_value_int64, libduckdb), Int64, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
	duckdb_value_hugeint(result,col,row)
 * returns: The duckdb_hugeint value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_hugeint(result, col, row)
    return ccall(
        (:duckdb_value_hugeint, libduckdb),
        Int64,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_uint8(result,col,row)
 * returns: The uint8_t value at the specified location, or 0 if the value cannot be converted.

"""
function duckdb_value_uint8(result, col, row)
    return ccall((:duckdb_value_uint8, libduckdb), UInt8, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
	duckdb_value_uint16(result,col,row)
 * returns: The uint16_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_uint16(result, col, row)
    return ccall(
        (:duckdb_value_uint16, libduckdb),
        UInt16,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_uint32(result,col,row)
 * returns: The uint32_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_uint32(result, col, row)
    return ccall(
        (:duckdb_value_uint32, libduckdb),
        UInt32,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_uint64(result,col,row)
* returns: The uint64_t value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_uint64(result, col, row)
    return ccall(
        (:duckdb_value_uint64, libduckdb),
        UInt64,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_float(result,col,row)
 * returns: The float value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_float(result, col, row)
    return ccall(
        (:duckdb_value_float, libduckdb),
        Float32,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
	duckdb_value_double(result,col,row)
 * returns: The double value at the specified location, or 0 if the value cannot be converted.
"""
function duckdb_value_double(result, col, row)
    return ccall(
        (:duckdb_value_double, libduckdb),
        Float64,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
duckdb_value_date(result,col,row)
 * returns: The duckdb_date value at the specified location, or 0 if the value cannot be converted.
DUCKDB_API duckdb_date duckdb_value_date(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_date(result, col, row)
    return ccall((:duckdb_value_date, libduckdb), Int32, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
duckdb_value_time(result,col,row)
 * returns: The duckdb_time value at the specified location, or 0 if the value cannot be converted.
DUCKDB_API duckdb_time duckdb_value_time(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_time(result, col, row)
    return ccall((:duckdb_value_time, libduckdb), Int32, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

"""
duckdb_value_timestamp(result,col,row)
 * returns: The duckdb_timestamp value at the specified location, or 0 if the value cannot be converted.
DUCKDB_API duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_timestamp(result, col, row)
    return ccall(
        (:duckdb_value_timestamp, libduckdb),
        Int32,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
duckdb_value_interval(result,col,row)
 * returns: The duckdb_interval value at the specified location, or 0 if the value cannot be converted.
DUCKDB_API duckdb_interval duckdb_value_interval(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_interval(result, col, row)
    return ccall(
        (:duckdb_value_interval, libduckdb),
        Int32,
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
duckdb_value_varchar(result,col,row)
* returns: The char* value at the specified location, or nullptr if the value cannot be converted.
The result must be freed with `duckdb_free`.
DUCKDB_API char *duckdb_value_varchar(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_varchar(result, col, row)
    return ccall(
        (:duckdb_value_varchar, libduckdb),
        Ptr{UInt8},
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

"""
duckdb_value_varchar_internal(result,col,row)
* returns: The char* value at the specified location. ONLY works on VARCHAR columns and does not auto-cast.
If the column is NOT a VARCHAR column this function will return NULL.
The result must NOT be freed.
DUCKDB_API char *duckdb_value_varchar_internal(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_varchar_internal(result, col, row)
    return ccall(
        (:duckdb_value_varchar_internal, libduckdb),
        Ptr{UInt8},
        (Ref{duckdb_result}, Int32, Int32),
        result,
        col - 1,
        row - 1
    )
end

# """
# duckdb_value_blob(result,col,row)
# * returns: The duckdb_blob value at the specified location. Returns a blob with blob.data set to nullptr if the
# value cannot be converted. The resulting "blob.data" must be freed with `duckdb_free.`
# DUCKDB_API duckdb_blob duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row);
# """
# function duckdb_value_blob(result, col, row)
#     return ccall(
#         (:duckdb_value_blob, libduckdb),
#         Ptr{Cvoid},
#         (Ptr{Cvoid}, Int32, Int32),
#         result,
#         col - 1,
#         row - 1,
#     )
# end

"""
duckdb_value_is_null(result,col,row)
 * returns: Returns true if the value at the specified index is NULL, and false otherwise.
DUCKDB_API bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
"""
function duckdb_value_is_null(result, col, row)
    return ccall((:duckdb_value_is_null, libduckdb), Bool, (Ref{duckdb_result}, Int32, Int32), result, col - 1, row - 1)
end

#=
//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
=#


"""
duckdb_malloc(size)

Allocate `size` bytes of memory using the duckdb internal malloc function. Any memory allocated in this manner
should be freed using `duckdb_free`.
* size: The number of bytes to allocate.
* returns: A pointer to the allocated memory region.
DUCKDB_API void *duckdb_malloc(size_t size);
"""
function duckdb_malloc(size)
    return ccall((:duckdb_malloc, libduckdb), Cvoid, (Csize_t,), size)
end

"""
duckdb_free(ptr)
Free a value returned from `duckdb_malloc`, `duckdb_value_varchar` or `duckdb_value_blob`.
* ptr: The memory region to de-allocate.
DUCKDB_API void duckdb_free(void *ptr);
"""
function duckdb_free(ptr)
    return ccall((:duckdb_free, libduckdb), Cvoid, (Ptr{Cvoid},), ptr)
end

"""
The internal vector size used by DuckDB.
This is the amount of tuples that will fit into a data chunk created by `duckdb_create_data_chunk`.

* returns: The vector size.
"""
function duckdb_vector_size()
    return ccall((:duckdb_vector_size, libduckdb), UInt64, ())
end

# #=
# //===--------------------------------------------------------------------===//
# // Date/Time/Timestamp Helpers
# //===--------------------------------------------------------------------===//
# =#
#
#
# """
# duckdb_from_date(date)
# Decompose a `duckdb_date` object into year, month and date (stored as `duckdb_date_struct`).
# * date: The date object, as obtained from a `DUCKDB_TYPE_DATE` column.
# * returns: The `duckdb_date_struct` with the decomposed elements.
# DUCKDB_API duckdb_date_struct duckdb_from_date(duckdb_date date);
# """
# function duckdb_from_date(date)
#     return ccall((:duckdb_from_date, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), date)
# end
#
# """
# duckdb_to_date(date)
# Re-compose a `duckdb_date` from year, month and date (`duckdb_date_struct`).
# * date: The year, month and date stored in a `duckdb_date_struct`.
# * returns: The `duckdb_date` element.
# DUCKDB_API duckdb_date duckdb_to_date(duckdb_date_struct date);
# """
# function duckdb_to_date(date)
#     return ccall((:duckdb_to_date, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), date)
# end
#
# """
# duckdb_from_time(time)
# Decompose a `duckdb_time` object into hour, minute, second and microsecond (stored as `duckdb_time_struct`).
# * time: The time object, as obtained from a `DUCKDB_TYPE_TIME` column.
# * returns: The `duckdb_time_struct` with the decomposed elements.
# DUCKDB_API duckdb_time_struct duckdb_from_time(duckdb_time time);
# """
# function duckdb_from_time(time)
#     return ccall((:duckdb_from_time, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), time)
# end
#
# """
# duckdb_to_time(time)
# Re-compose a `duckdb_time` from hour, minute, second and microsecond (`duckdb_time_struct`).
# * time: The hour, minute, second and microsecond in a `duckdb_time_struct`.
# * returns: The `duckdb_time` element.
# DUCKDB_API duckdb_time duckdb_to_time(duckdb_time_struct time);
# """
# function duckdb_to_time(time)
#     return ccall((:duckdb_to_time, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), time)
# end
#
# """
# duckdb_from_timestamp(ts)
# Decompose a `duckdb_timestamp` object into a `duckdb_timestamp_struct`.
# * ts: The ts object, as obtained from a `DUCKDB_TYPE_TIMESTAMP` column.
# * returns: The `duckdb_timestamp_struct` with the decomposed elements.
# DUCKDB_API duckdb_timestamp_struct duckdb_from_timestamp(duckdb_timestamp ts);
# """
# function duckdb_from_timestamp(ts)
#     return ccall((:duckdb_from_timestamp, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), ts)
# end
#
# """
# duckdb_to_timestamp(ts)
# Re-compose a `duckdb_timestamp` from a duckdb_timestamp_struct.
# * ts: The de-composed elements in a `duckdb_timestamp_struct`.
# * returns: The `duckdb_timestamp` element.
# */
# DUCKDB_API duckdb_timestamp duckdb_to_timestamp(duckdb_timestamp_struct ts);
# """
# function duckdb_to_timestamp(ts)
#     return ccall((:duckdb_to_timestamp, libduckdb), Ptr{Cvoid}, (Ptr{Cvoid},), ts)
# end
#
# #=
# //===--------------------------------------------------------------------===//
# // Hugeint Helpers
# //===--------------------------------------------------------------------===//
# =#
#
#
# """
# duckdb_hugeint_to_double(val)
# Converts a duckdb_hugeint object (as obtained from a `DUCKDB_TYPE_HUGEINT` column) into a double.
# * val: The hugeint value.
# * returns: The converted `double` element.
# DUCKDB_API double duckdb_hugeint_to_double(duckdb_hugeint val);
# """
# function duckdb_hugeint_to_double(val)
#     return ccall((:duckdb_hugeint_to_double, libduckdb), Float64, (Int64,), val)
# end
#
# """
# duckdb_double_to_hugeint(val)
# Converts a double value to a duckdb_hugeint object.
# If the conversion fails because the double value is too big the result will be 0.
# * val: The double value.
# * returns: The converted `duckdb_hugeint` element.
# DUCKDB_API duckdb_hugeint duckdb_double_to_hugeint(double val);
# """
# function duckdb_double_to_hugeint(val)
#     return ccall((:duckdb_double_to_hugeint, libduckdb), Int64, (Float64,), val)
# end
#
# #=
# //===--------------------------------------------------------------------===//
# // Prepared Statements
# //===--------------------------------------------------------------------===//
# // A prepared statement is a parameterized query that allows you to bind parameters to it.
# // * This is useful to easily supply parameters to functions and avoid SQL injection attacks.
# // * This is useful to speed up queries that you will execute several times with different parameters.
# // Because the query will only be parsed, bound, optimized and planned once during the prepare stage,
# // rather than once per execution.
# // For example:
# //   SELECT * FROM tbl WHERE id=?
# // Or a query with multiple parameters:
# //   SELECT * FROM tbl WHERE id=$1 OR name=$2
# =#


"""
Create a prepared statement object from a query.
Note that after calling `duckdb_prepare`, the prepared statement should always be destroyed using
`duckdb_destroy_prepare`, even if the prepare fails.
If the prepare fails, `duckdb_prepare_error` can be called to obtain the reason why the prepare failed.
* connection: The connection object
* query: The SQL query to prepare
* out_prepared_statement: The resulting prepared statement object
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_prepare(duckdb_connection connection, const char *query,
                                       duckdb_prepared_statement *out_prepared_statement);
"""
function duckdb_prepare(connection, query, out_prepared_statement)
    return ccall(
        (:duckdb_prepare, libduckdb),
        duckdb_state,
        (duckdb_connection, Ptr{UInt8}, Ref{duckdb_prepared_statement}),
        connection,
        query,
        out_prepared_statement
    )
end

"""
Closes the prepared statement and de-allocates all memory allocated for that connection.
* prepared_statement: The prepared statement to destroy.
DUCKDB_API void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);
"""
function duckdb_destroy_prepare(prepared_statement)
    return ccall((:duckdb_destroy_prepare, libduckdb), Cvoid, (Ref{duckdb_prepared_statement},), prepared_statement)
end

"""
Returns the error message associated with the given prepared statement.
If the prepared statement has no error message, this returns `nullptr` instead.
The error message should not be freed. It will be de-allocated when `duckdb_destroy_prepare` is called.
* prepared_statement: The prepared statement to obtain the error from.
* returns: The error message, or `nullptr` if there is none.
DUCKDB_API const char *duckdb_prepare_error(duckdb_prepared_statement prepared_statement);
"""
function duckdb_prepare_error(prepared_statement)
    return ccall((:duckdb_prepare_error, libduckdb), Ptr{UInt8}, (duckdb_prepared_statement,), prepared_statement[])
end

"""
Returns the number of parameters that can be provided to the given prepared statement.
Returns 0 if the query was not successfully prepared.
* prepared_statement: The prepared statement to obtain the number of parameters for.
DUCKDB_API idx_t duckdb_nparams(duckdb_prepared_statement prepared_statement);
"""
function duckdb_nparams(prepared_statement)
    return ccall((:duckdb_nparams, libduckdb), Int32, (duckdb_prepared_statement,), prepared_statement)
end

"""
Returns the parameter type for the parameter at the given index.
Returns `DUCKDB_TYPE_INVALID` if the parameter index is out of range or the statement was not successfully prepared.
* prepared_statement: The prepared statement.
* param_idx: The parameter index.
* returns: The parameter type
DUCKDB_API duckdb_type duckdb_param_type(duckdb_prepared_statement prepared_statement, idx_t param_idx);
"""
function duckdb_param_type(prepared_statement, param_idx)
    return ccall(
        (:duckdb_param_type, libduckdb),
        Int32,
        (duckdb_prepared_statement, Int32),
        prepared_statement,
        param_idx
    )
end

"""
Binds a bool value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
"""
function duckdb_bind_boolean(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_boolean, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an int8_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
"""
function duckdb_bind_int8(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_int8, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int8),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an int16_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
"""
function duckdb_bind_int16(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_int16, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int16),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an int32_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
"""
function duckdb_bind_int32(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_int32, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an int64_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
"""
function duckdb_bind_int64(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_int64, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int64),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an duckdb_hugeint value to the prepared statement at the specified index.
*/
DUCKDB_API duckdb_state duckdb_bind_hugeint(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            duckdb_hugeint val);
"""
function duckdb_bind_hugeint(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_hugeint, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, duckdb_hugeint),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an uint8_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);
"""
function duckdb_bind_uint8(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_uint8, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, UInt8),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an uint16_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);
"""
function duckdb_bind_uint16(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_uint16, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, UInt16),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an uint32_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
"""
function duckdb_bind_uint32(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_uint32, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, UInt32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an uint64_t value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
"""
function duckdb_bind_uint64(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_uint64, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, UInt64),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an float value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
"""
function duckdb_bind_float(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_float, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Float32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds an double value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
"""
function duckdb_bind_double(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_double, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Float64),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a duckdb_date value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_date(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         duckdb_date val);
"""
function duckdb_bind_date(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_date, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a duckdb_time value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_time(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         duckdb_time val);
"""
function duckdb_bind_time(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_time, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int64),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a duckdb_timestamp value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                              duckdb_timestamp val);
"""
function duckdb_bind_timestamp(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_timestamp, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int64),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a duckdb_interval value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_interval(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                             duckdb_interval val);
"""
function duckdb_bind_interval(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_interval, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Int32),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a null-terminated varchar value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                            const char *val);
"""
function duckdb_bind_varchar(prepared_statement, param_idx, val)
    return ccall(
        (:duckdb_bind_varchar, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Ptr{UInt8}),
        prepared_statement,
        param_idx,
        val
    )
end

"""
Binds a varchar value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_varchar_length(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                                   const char *val, idx_t length);
"""
function duckdb_bind_varchar_length(prepared_statement, param_idx, val, length)
    return ccall(
        (:duckdb_bind_varchar_length, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Ptr{UInt8}, Int32),
        prepared_statement,
        param_idx,
        val,
        length
    )
end

"""
Binds a blob value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx,
                                         const void *data, idx_t length);
"""
function duckdb_bind_blob(prepared_statement, param_idx, val, length)
    return ccall(
        (:duckdb_bind_blob, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32, Ptr{Cvoid}, Int32),
        prepared_statement,
        param_idx,
        data,
        length
    )
end

"""
Binds a NULL value to the prepared statement at the specified index.
DUCKDB_API duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);
"""
function duckdb_bind_null(prepared_statement, param_idx)
    return ccall(
        (:duckdb_bind_null, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Int32),
        prepared_statement,
        param_idx
    )
end

"""
Executes the prepared statement with the given bound parameters, and returns a materialized query result.
This method can be called multiple times for each prepared statement, and the parameters can be modified
between calls to this function.
* prepared_statement: The prepared statement to execute.
* out_result: The query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement,
                                                duckdb_result *out_result);
"""
function duckdb_execute_prepared(prepared_statement, out_result)
    return ccall(
        (:duckdb_execute_prepared, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Ref{duckdb_result}),
        prepared_statement,
        out_result
    )
end

# """
# Executes the prepared statement with the given bound parameters, and returns an arrow query result.
# * prepared_statement: The prepared statement to execute.
# * out_result: The query result.
# * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
# DUCKDB_API duckdb_state duckdb_execute_prepared_arrow(duckdb_prepared_statement prepared_statement,
#                                                       duckdb_arrow *out_result);
# """
# function duckdb_execute_prepared_arrow(prepared_statement, out_result)
#     return ccall(
#         (:duckdb_execute_prepared_arrow, libduckdb),
#         duckdb_state,
#         (Ptr{Cvoid}, Ptr{Cvoid}),
#         prepared_statement,
#         out_result,
#     )
# end

#=
//===--------------------------------------------------------------------===//
// Pending Result Interface
//===--------------------------------------------------------------------===//
=#
"""
Executes the prepared statement with the given bound parameters, and returns a pending result.
The pending result represents an intermediate structure for a query that is not yet fully executed.
The pending result can be used to incrementally execute a query, returning control to the client between tasks.

Note that after calling `duckdb_pending_prepared`, the pending result should always be destroyed using
`duckdb_destroy_pending`, even if this function returns DuckDBError.

* prepared_statement: The prepared statement to execute.
* out_result: The pending query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_pending_prepared(prepared_statement, out_pending)
    return ccall(
        (:duckdb_pending_prepared, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Ref{duckdb_pending_result}),
        prepared_statement,
        out_pending
    )
end

"""
Executes the prepared statement with the given bound parameters, and returns a pending result.
This pending result will create a streaming duckdb_result when executed.
The pending result represents an intermediate structure for a query that is not yet fully executed.

Note that after calling `duckdb_pending_prepared_streaming`, the pending result should always be destroyed using
`duckdb_destroy_pending`, even if this function returns DuckDBError.

* prepared_statement: The prepared statement to execute.
* out_result: The pending query result.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_pending_prepared_streaming(prepared_statement, out_pending)
    return ccall(
        (:duckdb_pending_prepared_streaming, libduckdb),
        duckdb_state,
        (duckdb_prepared_statement, Ref{duckdb_pending_result}),
        prepared_statement,
        out_pending
    )
end

"""
Closes the pending result and de-allocates all memory allocated for the result.

* pending_result: The pending result to destroy.
"""
function duckdb_destroy_pending(pending_result)
    return ccall((:duckdb_destroy_pending, libduckdb), Cvoid, (Ref{duckdb_pending_result},), pending_result)
end

"""
Returns the error message contained within the pending result.

The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_pending` is called.

* result: The pending result to fetch the error from.
* returns: The error of the pending result.
"""
function duckdb_pending_error(pending_result)
    return ccall((:duckdb_pending_error, libduckdb), Ptr{UInt8}, (duckdb_pending_result,), pending_result)
end

"""
Executes a single task within the query, returning whether or not the query is ready.

If this returns DUCKDB_PENDING_RESULT_READY, the duckdb_execute_pending function can be called to obtain the result.
If this returns DUCKDB_PENDING_RESULT_NOT_READY, the duckdb_pending_execute_task function should be called again.
If this returns DUCKDB_PENDING_ERROR, an error occurred during execution.

The error message can be obtained by calling duckdb_pending_error on the pending_result.

* pending_result: The pending result to execute a task within.
* returns: The state of the pending result after the execution.
"""
function duckdb_pending_execute_task(pending_result)
    return ccall(
        (:duckdb_pending_execute_task, libduckdb),
        duckdb_pending_state,
        (duckdb_pending_result,),
        pending_result
    )
end

"""
Fully execute a pending query result, returning the final query result.

If duckdb_pending_execute_task has been called until DUCKDB_PENDING_RESULT_READY was returned, this will return fast.
Otherwise, all remaining tasks must be executed first.

* pending_result: The pending result to execute.
* out_result: The result object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
"""
function duckdb_execute_pending(pending_result, out_result)
    return ccall(
        (:duckdb_execute_pending, libduckdb),
        duckdb_state,
        (duckdb_pending_result, Ref{duckdb_result}),
        pending_result,
        out_result
    )
end

"""
Returns whether a duckdb_pending_state is finished executing. For example if `pending_state` is
DUCKDB_PENDING_RESULT_READY, this function will return true.

* pending_state: The pending state on which to decide whether to finish execution.
* returns: Boolean indicating pending execution should be considered finished.
"""
function duckdb_pending_execution_is_finished(pending_state)
    return ccall((:duckdb_execute_pending, libduckdb), Bool, (duckdb_pending_state,), pending_state)
end

#=
//===--------------------------------------------------------------------===//
// Value Interface
//===--------------------------------------------------------------------===//
=#
"""
Destroys the value and de-allocates all memory allocated for that type.

* value: The value to destroy.
"""
function duckdb_destroy_value(handle)
    return ccall((:duckdb_destroy_value, libduckdb), Cvoid, (Ref{duckdb_value},), handle)
end

"""
Creates a value from a null-terminated string

* value: The null-terminated string
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
"""
function duckdb_create_varchar(handle)
    return ccall((:duckdb_create_varchar, libduckdb), duckdb_value, (Ptr{UInt8},), handle)
end

"""
Creates a value from a string

* value: The text
* length: The length of the text
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
"""
function duckdb_create_varchar_length(text, len)
    return ccall((:duckdb_create_varchar_length, libduckdb), duckdb_value, (Ptr{UInt8}, UInt64), text, len)
end


"""
Creates a value from an int64

* value: The bigint value
* returns: The value. This must be destroyed with `duckdb_destroy_value`.
"""
function duckdb_create_int64(handle)
    return ccall((:duckdb_create_int64, libduckdb), duckdb_value, (Int64,), handle)
end

"""
Obtains a string representation of the given value.
The result must be destroyed with `duckdb_free`.

* value: The value
* returns: The string value. This must be destroyed with `duckdb_free`.
"""
function duckdb_get_varchar(handle)
    return ccall((:duckdb_get_varchar, libduckdb), Ptr{UInt8}, (duckdb_value,), handle)
end

"""
Obtains an int64 of the given value.

* value: The value
* returns: The int64 value, or 0 if no conversion is possible
"""
function duckdb_get_int64(handle)
    return ccall((:duckdb_get_int64, libduckdb), Int64, (duckdb_value,), handle)
end

#=
//===--------------------------------------------------------------------===//
// Logical Type Interface
//===--------------------------------------------------------------------===//
=#
"""
Creates a `duckdb_logical_type` from a standard primitive type.
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

This should not be used with `DUCKDB_TYPE_DECIMAL`.

* type: The primitive type to create.
* returns: The logical type type.
"""
function duckdb_create_logical_type(type)
    return ccall((:duckdb_create_logical_type, libduckdb), duckdb_logical_type, (Int32,), type)
end

"""
Creates a `duckdb_logical_type` of type decimal with the specified width and scale
The resulting type should be destroyed with `duckdb_destroy_logical_type`.

* width: The width of the decimal type
* scale: The scale of the decimal type
* returns: The logical type.
"""
function duckdb_create_decimal_type(width, scale)
    return ccall((:duckdb_create_decimal_type, libduckdb), duckdb_logical_type, (UInt8, UInt8), width, scale)
end


"""
Retrieves the type class of a `duckdb_logical_type`.

* type: The logical type object
* returns: The type id
"""
function duckdb_get_type_id(handle)
    return ccall((:duckdb_get_type_id, libduckdb), DUCKDB_TYPE, (duckdb_logical_type,), handle)
end

"""
Retrieves the width of a decimal type.

* type: The logical type object
* returns: The width of the decimal type
"""
function duckdb_decimal_width(handle)
    return ccall((:duckdb_decimal_width, libduckdb), UInt8, (duckdb_logical_type,), handle)
end

"""
Retrieves the scale of a decimal type.

* type: The logical type object
* returns: The scale of the decimal type
"""
function duckdb_decimal_scale(handle)
    return ccall((:duckdb_decimal_scale, libduckdb), UInt8, (duckdb_logical_type,), handle)
end

"""
Retrieves the internal storage type of a decimal type.

* type: The logical type object
* returns: The internal type of the decimal type
"""
function duckdb_decimal_internal_type(handle)
    return ccall((:duckdb_decimal_internal_type, libduckdb), DUCKDB_TYPE, (duckdb_logical_type,), handle)
end

"""
Retrieves the internal storage type of an enum type.

* type: The logical type object
* returns: The internal type of the enum type
"""
function duckdb_enum_internal_type(handle)
    return ccall((:duckdb_enum_internal_type, libduckdb), DUCKDB_TYPE, (duckdb_logical_type,), handle)
end

"""
Retrieves the dictionary size of the enum type

* type: The logical type object
* returns: The dictionary size of the enum type
"""
function duckdb_enum_dictionary_size(handle)
    return ccall((:duckdb_enum_dictionary_size, libduckdb), UInt32, (duckdb_logical_type,), handle)
end

"""
Retrieves the dictionary value at the specified position from the enum.

The result must be freed with `duckdb_free`

* type: The logical type object
* index: The index in the dictionary
* returns: The string value of the enum type. Must be freed with `duckdb_free`.
"""
function duckdb_enum_dictionary_value(handle, index)
    return ccall(
        (:duckdb_enum_dictionary_value, libduckdb),
        Ptr{UInt8},
        (duckdb_logical_type, UInt64),
        handle,
        index - 1
    )
end

"""
Retrieves the child type of the given list type.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* returns: The child type of the list type. Must be destroyed with `duckdb_destroy_logical_type`.
"""
function duckdb_list_type_child_type(handle)
    return ccall((:duckdb_list_type_child_type, libduckdb), duckdb_logical_type, (duckdb_logical_type,), handle)
end

"""
Returns the number of children of a struct type.

* type: The logical type object
* returns: The number of children of a struct type.
"""
function duckdb_struct_type_child_count(handle)
    return ccall((:duckdb_struct_type_child_count, libduckdb), UInt64, (duckdb_logical_type,), handle)
end

"""
Returns the number of members of a union type.

* type: The logical type object
* returns: The number of members of a union type.
"""
function duckdb_union_type_member_count(handle)
    return ccall((:duckdb_union_type_member_count, libduckdb), UInt64, (duckdb_logical_type,), handle)
end

"""
Retrieves the name of the struct child.

The result must be freed with `duckdb_free`

* type: The logical type object
* index: The child index
* returns: The name of the struct type. Must be freed with `duckdb_free`.
"""
function duckdb_struct_type_child_name(handle, index)
    return ccall(
        (:duckdb_struct_type_child_name, libduckdb),
        Ptr{UInt8},
        (duckdb_logical_type, UInt64),
        handle,
        index - 1
    )
end

"""
Retrieves the name of the union member.

The result must be freed with `duckdb_free`

* type: The logical type object
* index: The member index
* returns: The name of the union member. Must be freed with `duckdb_free`.
"""
function duckdb_union_type_member_name(handle, index)
    return ccall(
        (:duckdb_union_type_member_name, libduckdb),
        Ptr{UInt8},
        (duckdb_logical_type, UInt64),
        handle,
        index - 1
    )
end

"""
Retrieves the child type of the given struct type at the specified index.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* index: The child index
* returns: The child type of the struct type. Must be destroyed with `duckdb_destroy_logical_type`.
"""
function duckdb_struct_type_child_type(handle, index)
    return ccall(
        (:duckdb_struct_type_child_type, libduckdb),
        duckdb_logical_type,
        (duckdb_logical_type, UInt64),
        handle,
        index - 1
    )
end

"""
Retrieves the member type of the given union type at the specified index.

The result must be freed with `duckdb_destroy_logical_type`

* type: The logical type object
* index: The member index
* returns: The member type of the union type. Must be destroyed with `duckdb_destroy_logical_type`.
"""
function duckdb_union_type_member_type(handle, index)
    return ccall(
        (:duckdb_union_type_member_type, libduckdb),
        duckdb_logical_type,
        (duckdb_logical_type, UInt64),
        handle,
        index - 1
    )
end

"""
Destroys the logical type and de-allocates all memory allocated for that type.

* type: The logical type to destroy.
"""
function duckdb_destroy_logical_type(handle)
    return ccall((:duckdb_destroy_logical_type, libduckdb), Cvoid, (Ref{duckdb_logical_type},), handle)
end

#=
//===--------------------------------------------------------------------===//
// Data Chunk Interface
//===--------------------------------------------------------------------===//
=#
"""
Creates an empty DataChunk with the specified set of types.

* types: An array of types of the data chunk.
* column_count: The number of columns.
* returns: The data chunk.
"""
function duckdb_create_data_chunk(types, column_count)
    return ccall(
        (:duckdb_create_data_chunk, libduckdb),
        duckdb_data_chunk,
        (Ptr{duckdb_logical_type}, UInt64),
        types,
        column_count
    )
end

"""
Destroys the data chunk and de-allocates all memory allocated for that chunk.

* chunk: The data chunk to destroy.
"""
function duckdb_destroy_data_chunk(chunk)
    return ccall((:duckdb_destroy_data_chunk, libduckdb), Cvoid, (Ref{duckdb_data_chunk},), chunk)
end

"""
Resets a data chunk, clearing the validity masks and setting the cardinality of the data chunk to 0.

* chunk: The data chunk to reset.
"""
function duckdb_data_chunk_reset(chunk)
    return ccall((:duckdb_data_chunk_reset, libduckdb), Cvoid, (duckdb_data_chunk,), chunk)
end

"""
Retrieves the number of columns in a data chunk.

* chunk: The data chunk to get the data from
* returns: The number of columns in the data chunk
"""
function duckdb_data_chunk_get_column_count(chunk)
    return ccall((:duckdb_data_chunk_get_column_count, libduckdb), UInt64, (duckdb_data_chunk,), chunk)
end


"""
Retrieves the current number of tuples in a data chunk.

* chunk: The data chunk to get the data from
* returns: The number of tuples in the data chunk
"""
function duckdb_data_chunk_get_size(chunk)
    return ccall((:duckdb_data_chunk_get_size, libduckdb), UInt64, (duckdb_data_chunk,), chunk)
end

"""
Sets the current number of tuples in a data chunk.

* chunk: The data chunk to set the size in
* size: The number of tuples in the data chunk
"""
function duckdb_data_chunk_set_size(chunk, size)
    return ccall((:duckdb_data_chunk_set_size, libduckdb), Cvoid, (duckdb_data_chunk, UInt64), chunk, size)
end

"""
Retrieves the vector at the specified column index in the data chunk.

The pointer to the vector is valid for as long as the chunk is alive.
It does NOT need to be destroyed.

* chunk: The data chunk to get the data from
* returns: The vector
"""
function duckdb_data_chunk_get_vector(chunk, col_idx)
    return ccall(
        (:duckdb_data_chunk_get_vector, libduckdb),
        duckdb_vector,
        (duckdb_data_chunk, UInt64),
        chunk,
        col_idx - 1
    )
end

#=
//===--------------------------------------------------------------------===//
// Vector Functions
//===--------------------------------------------------------------------===//
=#
"""
Retrieves the column type of the specified vector.

The result must be destroyed with `duckdb_destroy_logical_type`.

* vector: The vector get the data from
* returns: The type of the vector
"""
function duckdb_vector_get_column_type(vector)
    return ccall((:duckdb_vector_get_column_type, libduckdb), duckdb_logical_type, (duckdb_vector,), vector)
end

"""
Retrieves the data pointer of the vector.

The data pointer can be used to read or write values from the vector.
How to read or write values depends on the type of the vector.

* vector: The vector to get the data from
* returns: The data pointer
"""
function duckdb_vector_get_data(vector)
    return ccall((:duckdb_vector_get_data, libduckdb), Ptr{Cvoid}, (duckdb_vector,), vector)
end

"""
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
"""
function duckdb_vector_get_validity(vector)
    return ccall((:duckdb_vector_get_validity, libduckdb), Ptr{UInt64}, (duckdb_vector,), vector)
end

"""
Ensures the validity mask is writable by allocating it.

After this function is called, `duckdb_vector_get_validity` will ALWAYS return non-NULL.
This allows null values to be written to the vector, regardless of whether a validity mask was present before.

* vector: The vector to alter
"""
function duckdb_vector_ensure_validity_writable(vector)
    return ccall((:duckdb_vector_ensure_validity_writable, libduckdb), Cvoid, (duckdb_vector,), vector)
end

"""
Retrieves the child vector of a list vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* returns: The child vector
"""
function duckdb_list_vector_get_child(vector)
    return ccall((:duckdb_list_vector_get_child, libduckdb), duckdb_vector, (duckdb_vector,), vector)
end

"""
Returns the size of the child vector of the list

* vector: The vector
* returns: The size of the child list
"""
function duckdb_list_vector_get_size(vector)
    return ccall((:duckdb_list_vector_get_size, libduckdb), UInt64, (duckdb_vector,), vector)
end

"""
Retrieves the child vector of a struct vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* index: The child index
* returns: The child vector
"""
function duckdb_struct_vector_get_child(vector, index)
    return ccall(
        (:duckdb_struct_vector_get_child, libduckdb),
        duckdb_vector,
        (duckdb_vector, UInt64),
        vector,
        index - 1
    )
end

"""
Retrieves the member vector of a union vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* index: The member index
* returns: The member vector
"""
function duckdb_union_vector_get_member(vector, index)
    return ccall(
        (:duckdb_struct_vector_get_child, libduckdb),
        duckdb_vector,
        (duckdb_vector, UInt64),
        vector,
        1 + (index - 1)
    )
end

"""
Assigns a string element in the vector at the specified location.

* vector: The vector to alter
* index: The row position in the vector to assign the string to
* str: The null-terminated string
"""
function duckdb_vector_assign_string_element(vector, index, str)
    return ccall(
        (:duckdb_vector_assign_string_element, libduckdb),
        Cvoid,
        (duckdb_vector, UInt64, Ptr{UInt8}),
        vector,
        index - 1,
        str
    )
end
"""
Assigns a string element in the vector at the specified location.

* vector: The vector to alter
* index: The row position in the vector to assign the string to
* str: The null-terminated string
* str_len: The string length
"""
function duckdb_vector_assign_string_element_len(vector, index, str, str_len)
    return ccall(
        (:duckdb_vector_assign_string_element_len, libduckdb),
        Cvoid,
        (duckdb_vector, UInt64, Ptr{UInt8}, UInt64),
        vector,
        index - 1,
        str,
        str_len
    )
end

#=
//===--------------------------------------------------------------------===//
// Table Functions
//===--------------------------------------------------------------------===//
=#
"""
Creates a new empty table function.

The return value should be destroyed with `duckdb_destroy_table_function`.

* returns: The table function object.
"""
function duckdb_create_table_function()
    return ccall((:duckdb_create_table_function, libduckdb), duckdb_table_function, ())
end

"""
Destroys the given table function object.

* table_function: The table function to destroy
"""
function duckdb_destroy_table_function(func)
    return ccall((:duckdb_destroy_table_function, libduckdb), Cvoid, (Ref{duckdb_table_function},), func)
end

"""
Sets the name of the given table function.

* table_function: The table function
* name: The name of the table function
"""
function duckdb_table_function_set_name(func, name)
    return ccall((:duckdb_table_function_set_name, libduckdb), Cvoid, (duckdb_table_function, Ptr{UInt8}), func, name)
end

"""
Adds a parameter to the table function.

* table_function: The table function
* type: The type of the parameter to add.
"""
function duckdb_table_function_add_parameter(func, type)
    return ccall(
        (:duckdb_table_function_add_parameter, libduckdb),
        Cvoid,
        (duckdb_table_function, duckdb_logical_type),
        func,
        type
    )
end

"""
Assigns extra information to the table function that can be fetched during binding, etc.

* table_function: The table function
* extra_info: The extra information
* destroy: The callback that will be called to destroy the bind data (if any)
"""
function duckdb_table_function_set_extra_info(table_func, extra_data)
    return ccall(
        (:duckdb_table_function_set_extra_info, libduckdb),
        Cvoid,
        (duckdb_table_function, Ptr{Cvoid}, Ptr{Cvoid}),
        table_func,
        extra_data,
        C_NULL
    )
end

"""
Sets the bind function of the table function

* table_function: The table function
* bind: The bind function
"""
function duckdb_table_function_set_bind(table_func, bind_func)
    return ccall(
        (:duckdb_table_function_set_bind, libduckdb),
        Cvoid,
        (duckdb_table_function, Ptr{Cvoid}),
        table_func,
        bind_func
    )
end

"""
Sets the init function of the table function

* table_function: The table function
* init: The init function
"""
function duckdb_table_function_set_init(table_func, init_func)
    return ccall(
        (:duckdb_table_function_set_init, libduckdb),
        Cvoid,
        (duckdb_table_function, Ptr{Cvoid}),
        table_func,
        init_func
    )
end

"""
Sets the thread-local init function of the table function

* table_function: The table function
* init: The init function
"""
function duckdb_table_function_set_local_init(table_func, init_func)
    return ccall(
        (:duckdb_table_function_set_local_init, libduckdb),
        Cvoid,
        (duckdb_table_function, Ptr{Cvoid}),
        table_func,
        init_func
    )
end


"""
Sets the main function of the table function

* table_function: The table function
* function: The function
"""
function duckdb_table_function_set_function(table_func, func)
    return ccall(
        (:duckdb_table_function_set_function, libduckdb),
        Cvoid,
        (duckdb_table_function, Ptr{Cvoid}),
        table_func,
        func
    )
end

"""
Sets whether or not the given table function supports projection pushdown.

If this is set to true, the system will provide a list of all required columns in the `init` stage through
the `duckdb_init_get_column_count` and `duckdb_init_get_column_index` functions.
If this is set to false (the default), the system will expect all columns to be projected.

* table_function: The table function
* pushdown: True if the table function supports projection pushdown, false otherwise.
"""
function duckdb_table_function_supports_projection_pushdown(table_func, pushdown)
    return ccall(
        (:duckdb_table_function_supports_projection_pushdown, libduckdb),
        Cvoid,
        (duckdb_table_function, Bool),
        table_func,
        pushdown
    )
end

"""
Register the table function object within the given connection.

The function requires at least a name, a bind function, an init function and a main function.

If the function is incomplete or a function with this name already exists DuckDBError is returned.

* con: The connection to register it in.
* function: The function pointer
* returns: Whether or not the registration was successful.
"""
function duckdb_register_table_function(con, func)
    return ccall(
        (:duckdb_register_table_function, libduckdb),
        Int32,
        (duckdb_connection, duckdb_table_function),
        con,
        func
    )
end

#=
//===--------------------------------------------------------------------===//
// Table Function Bind
//===--------------------------------------------------------------------===//
=#

"""
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
"""
function duckdb_bind_get_extra_info(bind_info)
    return ccall((:duckdb_bind_get_extra_info, libduckdb), Ptr{Cvoid}, (duckdb_bind_info,), bind_info)
end

"""
Adds a result column to the output of the table function.

* info: The info object
* name: The name of the column
* type: The logical type of the column
"""
function duckdb_bind_add_result_column(bind_info, name, type)
    return ccall(
        (:duckdb_bind_add_result_column, libduckdb),
        Cvoid,
        (duckdb_bind_info, Ptr{UInt8}, duckdb_logical_type),
        bind_info,
        name,
        type
    )
end

"""
Retrieves the number of regular (non-named) parameters to the function.

* info: The info object
* returns: The number of parameters
"""
function duckdb_bind_get_parameter_count(bind_info)
    return ccall((:duckdb_bind_get_parameter_count, libduckdb), Int32, (duckdb_bind_info,), bind_info)
end

"""
Retrieves the parameter at the given index.

The result must be destroyed with `duckdb_destroy_value`.

* info: The info object
* index: The index of the parameter to get
* returns: The value of the parameter. Must be destroyed with `duckdb_destroy_value`.
"""
function duckdb_bind_get_parameter(bind_info, index)
    return ccall((:duckdb_bind_get_parameter, libduckdb), duckdb_value, (duckdb_bind_info, Int32), bind_info, index)
end

"""
Retrieves the parameter at the given index.

The result must be destroyed with `duckdb_destroy_value`.

* info: The info object
* index: The index of the parameter to get
* returns: The value of the parameter. Must be destroyed with `duckdb_destroy_value`.
"""
function duckdb_bind_set_bind_data(bind_info, bind_data, delete_callback)
    return ccall(
        (:duckdb_bind_set_bind_data, libduckdb),
        Cvoid,
        (duckdb_bind_info, Ptr{Cvoid}, Ptr{Cvoid}),
        bind_info,
        bind_data,
        delete_callback
    )
end

"""
Sets the cardinality estimate for the table function, used for optimization.

* info: The bind data object.
* is_exact: Whether or not the cardinality estimate is exact, or an approximation
"""
function duckdb_bind_set_cardinality(bind_info, cardinality, is_exact)
    return ccall(
        (:duckdb_bind_set_cardinality, libduckdb),
        Cvoid,
        (duckdb_bind_info, UInt64, Bool),
        bind_info,
        cardinality,
        is_exact
    )
end

"""
Report that an error has occurred during bind.

* info: The info object
* error: The error message
"""
function duckdb_bind_set_error(info, error_message)
    return ccall((:duckdb_bind_set_error, libduckdb), Cvoid, (duckdb_bind_info, Ptr{UInt8}), info, error_message)
end

#=
//===--------------------------------------------------------------------===//
// Table Function Init
//===--------------------------------------------------------------------===//
=#
"""
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
"""
function duckdb_init_get_extra_info(info)
    return ccall((:duckdb_init_get_extra_info, libduckdb), Ptr{Cvoid}, (duckdb_init_info,), info)
end

"""
Gets the bind data set by `duckdb_bind_set_bind_data` during the bind.

Note that the bind data should be considered as read-only.
For tracking state, use the init data instead.

* info: The info object
* returns: The bind data object
"""
function duckdb_init_get_bind_data(info)
    return ccall((:duckdb_init_get_bind_data, libduckdb), Ptr{Cvoid}, (duckdb_init_info,), info)
end

"""
Sets the user-provided init data in the init object. This object can be retrieved again during execution.

* info: The info object
* extra_data: The init data object.
* destroy: The callback that will be called to destroy the init data (if any)
"""
function duckdb_init_set_init_data(init_info, init_data, delete_callback)
    return ccall(
        (:duckdb_init_set_init_data, libduckdb),
        Cvoid,
        (duckdb_init_info, Ptr{Cvoid}, Ptr{Cvoid}),
        init_info,
        init_data,
        delete_callback
    )
end


"""
Returns the number of projected columns.

This function must be used if projection pushdown is enabled to figure out which columns to emit.

* info: The info object
* returns: The number of projected columns.
"""
function duckdb_init_get_column_count(info)
    return ccall((:duckdb_init_get_column_count, libduckdb), UInt64, (duckdb_init_info,), info)
end

"""
Returns the column index of the projected column at the specified position.

This function must be used if projection pushdown is enabled to figure out which columns to emit.

* info: The info object
* column_index: The index at which to get the projected column index, from 0..duckdb_init_get_column_count(info)
* returns: The column index of the projected column.
"""
function duckdb_init_get_column_index(info, index)
    return ccall((:duckdb_init_get_column_index, libduckdb), UInt64, (duckdb_init_info, UInt64), info, index - 1) + 1
end

"""
Sets how many threads can process this table function in parallel (default: 1)

* info: The info object
* max_threads: The maximum amount of threads that can process this table function
"""
function duckdb_init_set_max_threads(info, max_threads)
    return ccall((:duckdb_init_set_max_threads, libduckdb), Cvoid, (duckdb_init_info, UInt64), info, max_threads)
end

"""
Report that an error has occurred during init.

* info: The info object
* error: The error message
"""
function duckdb_init_set_error(info, error_message)
    return ccall((:duckdb_init_set_error, libduckdb), Cvoid, (duckdb_init_info, Ptr{UInt8}), info, error_message)
end

#=
//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
=#
"""
Retrieves the extra info of the function as set in `duckdb_table_function_set_extra_info`

* info: The info object
* returns: The extra info
"""
function duckdb_function_get_extra_info(info)
    return ccall((:duckdb_function_get_extra_info, libduckdb), Ptr{Cvoid}, (duckdb_function_info,), info)
end

"""
Gets the bind data set by `duckdb_bind_set_bind_data` during the bind.

Note that the bind data should be considered as read-only.
For tracking state, use the init data instead.

* info: The info object
* returns: The bind data object
"""
function duckdb_function_get_bind_data(info)
    return ccall((:duckdb_function_get_bind_data, libduckdb), Ptr{Cvoid}, (duckdb_function_info,), info)
end

"""
Gets the init data set by `duckdb_init_set_init_data` during the init.

* info: The info object
* returns: The init data object
"""
function duckdb_function_get_init_data(info)
    return ccall((:duckdb_function_get_init_data, libduckdb), Ptr{Cvoid}, (duckdb_function_info,), info)
end

"""
Gets the init data set by `duckdb_init_set_init_data` during the local_init.

* info: The info object
* returns: The init data object
"""
function duckdb_function_get_local_init_data(info)
    return ccall((:duckdb_function_get_local_init_data, libduckdb), Ptr{Cvoid}, (duckdb_function_info,), info)
end

"""
Report that an error has occurred while executing the function.

* info: The info object
* error: The error message
"""
function duckdb_function_set_error(info, error_message)
    return ccall(
        (:duckdb_function_set_error, libduckdb),
        Cvoid,
        (duckdb_function_info, Ptr{UInt8}),
        info,
        error_message
    )
end

#=
//===--------------------------------------------------------------------===//
// Replacement scans
//===--------------------------------------------------------------------===//
=#
"""
Add a replacement scan definition to the specified database

* db: The database object to add the replacement scan to
* replacement: The replacement scan callback
* extra_data: Extra data that is passed back into the specified callback
* delete_callback: The delete callback to call on the extra data, if any
"""
function duckdb_add_replacement_scan(db, replacement, extra_data, delete_callback)
    return ccall(
        (:duckdb_add_replacement_scan, libduckdb),
        Cvoid,
        (duckdb_database, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}),
        db,
        replacement,
        extra_data,
        delete_callback
    )
end

"""
Sets the replacement function name to use. If this function is called in the replacement callback,
 the replacement scan is performed. If it is not called, the replacement callback is not performed.

* info: The info object
* function_name: The function name to substitute.
"""
function duckdb_replacement_scan_set_function_name(info, function_name)
    return ccall(
        (:duckdb_replacement_scan_set_function_name, libduckdb),
        Cvoid,
        (duckdb_replacement_scan_info, Ptr{UInt8}),
        info,
        function_name
    )
end


"""
Adds a parameter to the replacement scan function.

* info: The info object
* parameter: The parameter to add. The function will call `duckdb_destroy_value` on the parameter.
"""
function duckdb_replacement_scan_add_parameter(info, parameter)
    return ccall(
        (:duckdb_replacement_scan_add_parameter, libduckdb),
        Cvoid,
        (duckdb_replacement_scan_info, duckdb_value),
        info,
        parameter
    )
end

"""
Report that an error has occurred while executing the replacement scan.

* info: The info object
* error: The error message
"""
function duckdb_replacement_scan_set_error(info, error_message)
    return ccall(
        (:duckdb_replacement_scan_set_error, libduckdb),
        Cvoid,
        (duckdb_replacement_scan_info, Ptr{UInt8}),
        info,
        error_message
    )
end

#=
//===--------------------------------------------------------------------===//
// Appender
//===--------------------------------------------------------------------===//
=#

"""
Creates an appender object.
* connection: The connection context to create the appender in.
* schema: The schema of the table to append to, or `nullptr` for the default schema.
* table: The table name to append to.
* out_appender: The resulting appender object.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table,
                                               duckdb_appender *out_appender);
"""
function duckdb_appender_create(connection, schema, table, out_appender)
    return ccall(
        (:duckdb_appender_create, libduckdb),
        Int32,
        (duckdb_connection, Ptr{UInt8}, Ptr{UInt8}, Ref{duckdb_appender}),
        connection,
        schema,
        table,
        out_appender
    )
end

"""
Returns the error message associated with the given appender.
If the appender has no error message, this returns `nullptr` instead.
The error message should not be freed. It will be de-allocated when `duckdb_appender_destroy` is called.
* appender: The appender to get the error from.
* returns: The error message, or `nullptr` if there is none.
DUCKDB_API const char *duckdb_appender_error(duckdb_appender appender);
"""
function duckdb_appender_error(appender)
    return ccall((:duckdb_appender_error, libduckdb), Ptr{UInt8}, (duckdb_appender,), appender)
end

"""
Flush the appender to the table, forcing the cache of the appender to be cleared and the data to be appended to the
base table.
This should generally not be used unless you know what you are doing. Instead, call `duckdb_appender_destroy` when you
are done with the appender.
* appender: The appender to flush.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_appender_flush(duckdb_appender appender);
"""
function duckdb_appender_flush(appender)
    return ccall((:duckdb_appender_flush, libduckdb), duckdb_state, (duckdb_appender,), appender)
end

"""
Close the appender, flushing all intermediate state in the appender to the table and closing it for further appends.
This is generally not necessary. Call `duckdb_appender_destroy` instead.
* appender: The appender to flush and close.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_appender_close(duckdb_appender appender);
"""
function duckdb_appender_close(appender)
    return ccall((:duckdb_appender_close, libduckdb), duckdb_state, (duckdb_appender,), appender)
end

"""
Close the appender and destroy it. Flushing all intermediate state in the appender to the table, and de-allocating
all memory associated with the appender.
* appender: The appender to flush, close and destroy.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_appender_destroy(duckdb_appender *appender);
"""
function duckdb_appender_destroy(appender)
    return ccall((:duckdb_appender_destroy, libduckdb), duckdb_state, (Ref{duckdb_appender},), appender)
end

"""
A nop function, provided for backwards compatibility reasons. Does nothing. Only `duckdb_appender_end_row` is required.
DUCKDB_API duckdb_state duckdb_appender_begin_row(duckdb_appender appender);
"""
function duckdb_appender_begin_row(appender)
    return ccall((:duckdb_appender_begin_row, libduckdb), duckdb_state, (duckdb_appender,), appender)
end

"""
Finish the current row of appends. After end_row is called, the next row can be appended.
* appender: The appender.
* returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
DUCKDB_API duckdb_state duckdb_appender_end_row(duckdb_appender appender);
"""
function duckdb_appender_end_row(appender)
    return ccall((:duckdb_appender_end_row, libduckdb), duckdb_state, (duckdb_appender,), appender)
end

"""
Append a bool value to the appender.
DUCKDB_API duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);
"""
function duckdb_append_bool(appender, value)
    return ccall((:duckdb_append_bool, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append an int8_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_int8(duckdb_appender appender, int8_t value);
"""
function duckdb_append_int8(appender, value)
    return ccall((:duckdb_append_int8, libduckdb), duckdb_state, (duckdb_appender, Int16), appender, value)
end

"""
Append an int16_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_int16(duckdb_appender appender, int16_t value);
"""
function duckdb_append_int16(appender, value)
    return ccall((:duckdb_append_int16, libduckdb), duckdb_state, (duckdb_appender, Int16), appender, value)
end

"""
Append an int32_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
"""
function duckdb_append_int32(appender, value)
    return ccall((:duckdb_append_int16, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append an int64_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);
"""
function duckdb_append_int64(appender, value)
    return ccall((:duckdb_append_int64, libduckdb), duckdb_state, (duckdb_appender, Int64), appender, value)
end

"""
Append a duckdb_hugeint value to the appender.
DUCKDB_API duckdb_state duckdb_append_hugeint(duckdb_appender appender, duckdb_hugeint value);
"""
function duckdb_append_hugeint(appender, value)
    return ccall((:duckdb_append_hugeint, libduckdb), duckdb_state, (duckdb_appender, Int64), appender, value)
end

"""
Append a uint8_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_uint8(duckdb_appender appender, uint8_t value);
"""
function duckdb_append_uint8(appender, value)
    return ccall((:duckdb_append_uint8, libduckdb), duckdb_state, (duckdb_appender, UInt16), appender, value)
end

"""
Append a uint16_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_uint16(duckdb_appender appender, uint16_t value);
"""
function duckdb_append_uint16(appender, value)
    return ccall((:duckdb_append_uint16, libduckdb), duckdb_state, (duckdb_appender, UInt16), appender, value)
end

"""
Append a uint32_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_uint32(duckdb_appender appender, uint32_t value);
"""
function duckdb_append_uint32(appender, value)
    return ccall((:duckdb_append_uint32, libduckdb), duckdb_state, (duckdb_appender, UInt32), appender, value)
end

"""
Append a uint64_t value to the appender.
DUCKDB_API duckdb_state duckdb_append_uint64(duckdb_appender appender, uint64_t value);
"""
function duckdb_append_uint64(appender, value)
    return ccall((:duckdb_append_uint64, libduckdb), duckdb_state, (duckdb_appender, UInt64), appender, value)
end

"""
Append a float value to the appender.
DUCKDB_API duckdb_state duckdb_append_float(duckdb_appender appender, float value);
"""
function duckdb_append_float(appender, value)
    return ccall((:duckdb_append_float, libduckdb), duckdb_state, (duckdb_appender, Float32), appender, value)
end

"""
Append a double value to the appender.
DUCKDB_API duckdb_state duckdb_append_double(duckdb_appender appender, double value);
"""
function duckdb_append_double(appender, value)
    return ccall((:duckdb_append_double, libduckdb), duckdb_state, (duckdb_appender, Float64), appender, value)
end

"""
Append a duckdb_date value to the appender.
DUCKDB_API duckdb_state duckdb_append_date(duckdb_appender appender, duckdb_date value);
"""
function duckdb_append_date(appender, value)
    return ccall((:duckdb_append_date, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append a duckdb_time value to the appender.
DUCKDB_API duckdb_state duckdb_append_time(duckdb_appender appender, duckdb_time value);
"""
function duckdb_append_time(appender, value)
    return ccall((:duckdb_append_time, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append a duckdb_timestamp value to the appender.
DUCKDB_API duckdb_state duckdb_append_timestamp(duckdb_appender appender, duckdb_timestamp value);
"""
function duckdb_append_timestamp(appender, value)
    return ccall((:duckdb_append_timestamp, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append a duckdb_interval value to the appender.
DUCKDB_API duckdb_state duckdb_append_interval(duckdb_appender appender, duckdb_interval value);
"""
function duckdb_append_interval(appender, value)
    return ccall((:duckdb_append_interval, libduckdb), duckdb_state, (duckdb_appender, Int32), appender, value)
end

"""
Append a varchar value to the appender.
DUCKDB_API duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *val);
"""
function duckdb_append_varchar(appender, value)
    return ccall((:duckdb_append_varchar, libduckdb), duckdb_state, (duckdb_appender, Ptr{UInt8}), appender, value)
end

"""
Append a varchar value to the appender.
DUCKDB_API duckdb_state duckdb_append_varchar_length(duckdb_appender appender, const char *val, idx_t length);
"""
function duckdb_append_varchar_length(appender, value, length)
    return ccall(
        (:duckdb_append_varchar_length, libduckdb),
        duckdb_state,
        (duckdb_appender, Ptr{UInt8}, Int32),
        appender,
        value,
        length
    )
end

"""
Append a blob value to the appender.
DUCKDB_API duckdb_state duckdb_append_blob(duckdb_appender appender, const void *data, idx_t length);
"""
function duckdb_append_blob(appender, data, length)
    return ccall(
        (:duckdb_append_blob, libduckdb),
        duckdb_state,
        (duckdb_appender, Ptr{Cvoid}, Int32),
        appender,
        data,
        length
    )
end

"""
Append a NULL value to the appender (of any type).
DUCKDB_API duckdb_state duckdb_append_null(duckdb_appender appender);
"""
function duckdb_append_null(appender)
    return ccall((:duckdb_append_null, libduckdb), duckdb_state, (duckdb_appender,), appender)
end

# #=
# //===--------------------------------------------------------------------===//
# // Arrow Interface
# //===--------------------------------------------------------------------===//
# =#
#
#
# """
# Executes a SQL query within a connection and stores the full (materialized) result in an arrow structure.
# If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
# `duckdb_query_arrow_error`.
# Note that after running `duckdb_query_arrow`, `duckdb_destroy_arrow` must be called on the result object even if the
# query fails, otherwise the error stored within the result will not be freed correctly.
# * connection: The connection to perform the query in.
# * query: The SQL query to run.
# * out_result: The query result.
# * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
# DUCKDB_API duckdb_state duckdb_query_arrow(duckdb_connection connection, const char *query, duckdb_arrow *out_result);
# """
# function duckdb_query_arrow(connection, query, out_result)
#     return ccall(
#         (:duckdb_query_arrow, libduckdb),
#         Int32,
#         (Ptr{Cvoid}, Ptr{UInt8}, Ptr{Cvoid}),
#         connection[],
#         query,
#         out_result,
#     )
# end
#
# """
# Fetch the internal arrow schema from the arrow result.
# * result: The result to fetch the schema from.
# * out_schema: The output schema.
# * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
# DUCKDB_API duckdb_state duckdb_query_arrow_schema(duckdb_arrow result, duckdb_arrow_schema *out_schema);
# """
# function duckdb_query_arrow_schema(result, out_schema)
#     return ccall(
#         (:duckdb_query_arrow_schema, libduckdb),
#         duckdb_state,
#         (Ptr{Cvoid}, Ptr{UInt8}),
#         result,
#         out_schema,
#     )
# end
#
# """
# Fetch an internal arrow array from the arrow result.
# This function can be called multiple time to get next chunks, which will free the previous out_array.
# So consume the out_array before calling this function again.
# * result: The result to fetch the array from.
# * out_array: The output array.
# * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
# DUCKDB_API duckdb_state duckdb_query_arrow_array(duckdb_arrow result, duckdb_arrow_array *out_array);
# """
# function duckdb_query_arrow_array(result, out_array)
#     return ccall(
#         (:duckdb_query_arrow_array, libduckdb),
#         duckdb_state,
#         (Ptr{Cvoid}, Ptr{Cvoid}),
#         result,
#         out_array,
#     )
# end
#
# """
# Returns the number of columns present in a the arrow result object.
# * result: The result object.
# * returns: The number of columns present in the result object.
# DUCKDB_API idx_t duckdb_arrow_column_count(duckdb_arrow result);
# """
# function duckdb_arrow_column_count(result)
#     return ccall((:duckdb_arrow_column_count, libduckdb), Int32, (Ptr{Cvoid},), result)
# end
#
# """
# Returns the number of rows present in a the arrow result object.
# * result: The result object.
# * returns: The number of rows present in the result object.
# DUCKDB_API idx_t duckdb_arrow_row_count(duckdb_arrow result);
# """
# function duckdb_arrow_row_count(result)
#     return ccall((:duckdb_arrow_row_count, libduckdb), Int64, (Ptr{Cvoid},), result)
# end
#
# """
# Returns the number of rows changed by the query stored in the arrow result. This is relevant only for
# INSERT/UPDATE/DELETE queries. For other queries the rows_changed will be 0.
# * result: The result object.
# * returns: The number of rows changed.
# DUCKDB_API idx_t duckdb_arrow_rows_changed(duckdb_arrow result);
# """
# function duckdb_arrow_rows_changed(result)
#     return ccall((:duckdb_arrow_rows_changed, libduckdb), Int64, (Ptr{Cvoid},), result)
# end
#
# """
# Returns the error message contained within the result. The error is only set if `duckdb_query_arrow` returns
# `DuckDBError`.
# The error message should not be freed. It will be de-allocated when `duckdb_destroy_arrow` is called.
# * result: The result object to fetch the nullmask from.
# * returns: The error of the result.
# DUCKDB_API const char *duckdb_query_arrow_error(duckdb_arrow result);
# """
# function duckdb_query_arrow_error(result)
#     return ccall((:duckdb_query_arrow_error, libduckdb), Ptr{UInt8}, (Ptr{Cvoid},), result)
# end
#
# """
# Closes the result and de-allocates all memory allocated for the arrow result.
# * result: The result to destroy.
# DUCKDB_API void duckdb_destroy_arrow(duckdb_arrow *result);
# """
# function duckdb_destroy_arrow(result)
#     return ccall((:duckdb_destroy_arrow, libduckdb), Cvoid, (Ptr{Ptr{Cvoid}},), result)
# end

#=
//===--------------------------------------------------------------------===//
// Threading Interface
//===--------------------------------------------------------------------===//
=#
"""
Execute DuckDB tasks on this thread.

Will return after `max_tasks` have been executed, or if there are no more tasks present.

* database: The database object to execute tasks for
* max_tasks: The maximum amount of tasks to execute
"""
function duckdb_execute_tasks(handle, max_tasks)
    return ccall((:duckdb_execute_tasks, libduckdb), Cvoid, (duckdb_database, UInt64), handle, max_tasks)
end

"""
Creates a task state that can be used with duckdb_execute_tasks_state to execute tasks until
 duckdb_finish_execution is called on the state.

duckdb_destroy_state should be called on the result in order to free memory.

* returns: The task state that can be used with duckdb_execute_tasks_state.
"""
function duckdb_create_task_state(database)
    return ccall((:duckdb_create_task_state, libduckdb), duckdb_task_state, (duckdb_database,), database)
end

"""
Execute DuckDB tasks on this thread.

The thread will keep on executing tasks forever, until duckdb_finish_execution is called on the state.
Multiple threads can share the same duckdb_task_state.

* database: The database object to execute tasks for
* state: The task state of the executor
"""
function duckdb_execute_tasks_state(state)
    return ccall((:duckdb_execute_tasks_state, libduckdb), Cvoid, (duckdb_task_state,), state)
end

"""
Execute DuckDB tasks on this thread.

The thread will keep on executing tasks until either duckdb_finish_execution is called on the state,
max_tasks tasks have been executed or there are no more tasks to be executed.

Multiple threads can share the same duckdb_task_state.

* state: The task state of the executor
* max_tasks: The maximum amount of tasks to execute
* returns: The amount of tasks that have actually been executed
"""
function duckdb_execute_n_tasks_state(state, max_tasks)
    return ccall((:duckdb_execute_n_tasks_state, libduckdb), UInt64, (duckdb_task_state, UInt64), state, max_tasks)
end

"""
Finish execution on a specific task.

* state: The task state to finish execution
"""
function duckdb_finish_execution(state)
    return ccall((:duckdb_finish_execution, libduckdb), Cvoid, (duckdb_task_state,), state)
end

"""
Check if the provided duckdb_task_state has finished execution

* state: The task state to inspect
* returns: Whether or not duckdb_finish_execution has been called on the task state
"""
function duckdb_task_state_is_finished(state)
    return ccall((:duckdb_task_state_is_finished, libduckdb), Bool, (duckdb_task_state,), state)
end

"""
Destroys the task state returned from duckdb_create_task_state.

Note that this should not be called while there is an active duckdb_execute_tasks_state running
on the task state.

* state: The task state to clean up
"""
function duckdb_destroy_task_state(state)
    return ccall((:duckdb_destroy_task_state, libduckdb), Cvoid, (duckdb_task_state,), state)
end

"""
Returns true if execution of the current query is finished.

* con: The connection on which to check
"""
function duckdb_execution_is_finished(con)
    return ccall((:duckdb_execution_is_finished, libduckdb), Bool, (duckdb_connection,), con)
end
