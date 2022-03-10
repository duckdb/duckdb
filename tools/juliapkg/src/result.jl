using DataFrames
using Tables

mutable struct QueryResult
    handle::Ref{duckdb_result}
    df::DataFrame

    function QueryResult(handle::Ref{duckdb_result})
        df = toDataFrame(handle)

        result = new(handle, df)
        finalizer(_close_result, result)
        return result
    end
end

function _close_result(result::QueryResult)
    return duckdb_destroy_result(result.handle)
end

function nop_convert(val)
	return val
end

function convert_string(val::Ptr{Cvoid}, idx::UInt64)
	base_ptr = val + (idx - 1) * sizeof(duckdb_string_t)
    length_ptr = Base.unsafe_convert(Ptr{Int32}, base_ptr)
	length = unsafe_load(length_ptr)
	if length <= STRING_INLINE_LENGTH
		prefix_ptr = Base.unsafe_convert(Ptr{UInt8}, base_ptr + sizeof(Int32))
		return unsafe_string(prefix_ptr, length)
	else
		ptr_ptr = Base.unsafe_convert(Ptr{Ptr{UInt8}}, base_ptr + sizeof(Int32) * 2)
		data_ptr = Base.unsafe_load(ptr_ptr)
		return unsafe_string(data_ptr, length)
	end
end

function convert_date(val::Int32)::Date
	return Dates.epochdays2date(val + 719528)
end

function convert_time(val::Int64)::Time
	return Dates.Time(Dates.Nanosecond(val * 1000))
end

function convert_timestamp(val::Int64)::DateTime
	return Dates.epochms2datetime((val ÷ 1000) + 62167219200000)
end

function convert_timestamp_s(val::Int64)::DateTime
	return Dates.epochms2datetime((val * 1000) + 62167219200000)
end

function convert_timestamp_ms(val::Int64)::DateTime
	return Dates.epochms2datetime((val) + 62167219200000)
end

function convert_timestamp_ns(val::Int64)::DateTime
	return Dates.epochms2datetime((val ÷ 1000000) + 62167219200000)
end

function convert_interval(val::duckdb_interval)::Dates.CompoundPeriod
	return Dates.CompoundPeriod(
		Dates.Month(val.months),
		Dates.Day(val.days),
		Dates.Microsecond(val.micros),
	)
end

function convert_hugeint(val::duckdb_hugeint)::Int128
	return Int128(val.lower) + Int128(val.upper) << 64;
end

function convert_chunk(chunk::DataChunk, col_idx::Int64, convert_func::Function, result, position, all_valid, ::Type{SRC}, ::Type{DST}) where {SRC, DST}
	size = GetSize(chunk)
	array = GetArray(chunk, col_idx, SRC)
	if !all_valid
		validity = GetValidity(chunk, col_idx)
	end
	for i in 1:size
		if all_valid || IsValid(validity, i)
			result[position] = convert_func(array[i])
		end
		position += 1
	end
	return size
end

function convert_chunk_string(chunk::DataChunk, col_idx::Int64, convert_func::Function, result, position, all_valid, ::Type{SRC}, ::Type{DST}) where {SRC, DST}
	size = GetSize(chunk)
    raw_ptr = duckdb_data_chunk_get_data(chunk.handle, col_idx)
    ptr = Base.unsafe_convert(Ptr{duckdb_string_t}, raw_ptr)
    if !all_valid
		validity = GetValidity(chunk, col_idx)
	end
	for i in 1:size
		if all_valid || IsValid(validity, i)
 			result[position] = convert_func(raw_ptr, i)
		end
		position += 1
	end
	return size
end

function convert_column_loop(chunks::Vector{DataChunk}, col_idx::Int64, convert_func::Function, ::Type{SRC}, ::Type{DST}, convert_chunk_func::Function = convert_chunk) where {SRC, DST}
	# first check if there are null values in any chunks
	has_missing = false
	row_count = 0
	for chunk in chunks
		if !AllValid(chunk, col_idx)
			has_missing = true
		end
		row_count += GetSize(chunk)
	end
	if has_missing
		# missing values
		result = Array{Union{Missing,DST}}(missing, row_count)
		position = 1
		for chunk in chunks
			position += convert_chunk_func(chunk, col_idx, convert_func, result, position, AllValid(chunk, col_idx), SRC, DST)
		end
	else
		# no missing values
		result = Array{DST}(undef, row_count)
		position = 1
		for chunk in chunks
			position += convert_chunk_func(chunk, col_idx, convert_func, result, position, true, SRC, DST)
		end
	end
	return result
end

function standard_convert(chunks::Vector{DataChunk}, col_idx::Int64, ::Type{T}) where {T}
	return convert_column_loop(chunks, col_idx, nop_convert, T, T)
end

function convert_column(chunks::Vector{DataChunk}, col_idx::Int64, logical_type::LogicalType)
	type = GetTypeId(logical_type)
	internal_type_id = GetInternalTypeId(logical_type)
	internal_type = duckdb_type_to_internal_type(internal_type_id)

	if type == DUCKDB_TYPE_VARCHAR
		return convert_column_loop(chunks, col_idx, convert_string, duckdb_string_t, AbstractString, convert_chunk_string)
	elseif type == DUCKDB_TYPE_DATE
		return convert_column_loop(chunks, col_idx, convert_date, internal_type, Date)
	elseif type == DUCKDB_TYPE_TIME
		return convert_column_loop(chunks, col_idx, convert_time, internal_type, Time)
	elseif type == DUCKDB_TYPE_TIMESTAMP
		return convert_column_loop(chunks, col_idx, convert_timestamp, internal_type, DateTime)
	elseif type == DUCKDB_TYPE_TIMESTAMP_S
		return convert_column_loop(chunks, col_idx, convert_timestamp_s, internal_type, DateTime)
	elseif type == DUCKDB_TYPE_TIMESTAMP_MS
		return convert_column_loop(chunks, col_idx, convert_timestamp_ms, internal_type, DateTime)
	elseif type == DUCKDB_TYPE_TIMESTAMP_NS
		return convert_column_loop(chunks, col_idx, convert_timestamp_ns, internal_type, DateTime)
	elseif type == DUCKDB_TYPE_INTERVAL
		return convert_column_loop(chunks, col_idx, convert_interval, internal_type, Dates.CompoundPeriod)
	elseif type == DUCKDB_TYPE_HUGEINT
		return convert_column_loop(chunks, col_idx, convert_hugeint, internal_type, Int128)
	elseif type == DUCKDB_TYPE_DECIMAL
		if internal_type_id == DUCKDB_TYPE_HUGEINT
			column = convert_column_loop(chunks, col_idx, convert_hugeint, internal_type, Int128)
		else
			column = standard_convert(chunks, col_idx, internal_type)
		end
		scale = 10 ^ GetDecimalScale(logical_type)
		column = column ./ scale
		return column
	elseif type == DUCKDB_TYPE_ENUM
		column = standard_convert(chunks, col_idx, internal_type)
		dictionary = GetEnumDictionary(logical_type)
		return map(x -> x === missing ? missing : dictionary[x + 1], column)
	else
		return standard_convert(chunks, col_idx, internal_type)
	end
end

function toDataFrame(result::Ref{duckdb_result})::DataFrame
    column_count = duckdb_column_count(result)
	# duplicate eliminate the names
	names = Vector{Symbol}(undef, column_count)
	for i in 1:column_count
	  name = sym(duckdb_column_name(result, i))
	  if name in view(names, 1:(i - 1))
		  j = 1
		  new_name = Symbol(name, :_, j)
		  while new_name in view(names, 1:(i - 1))
			  j += 1
			  new_name = Symbol(name, :_, j)
		  end
		  name = new_name
	  end
	  names[i] = name
	end
	# gather all the data chunks
	chunk_count = duckdb_result_chunk_count(result[])
	chunks::Vector{DataChunk} = []
	for i = 1:chunk_count
		push!(chunks, DataChunk(duckdb_result_get_chunk(result[], i), true))
	end

    df = DataFrame()
    for i = 1:column_count
        name = names[i]
        logical_type = LogicalType(duckdb_column_logical_type(result, i))
        df[!, name] = convert_column(chunks, i, logical_type)
    end
    return df
end

function execute(stmt::Stmt, params::DBInterface.StatementParams = ())
    BindParameters(stmt, params)

    handle = Ref{duckdb_result}()
    if duckdb_execute_prepared(stmt.handle, handle) != DuckDBSuccess
        error_ptr = duckdb_result_error(handle)
        if error_ptr == C_NULL
            error_message = string("Execute of query \"", stmt.sql, "\" failed: unknown error")
        else
            error_message = string("Execute of query \"", stmt.sql, "\" failed: ", unsafe_string(error_ptr))
        end
        duckdb_destroy_result(handle)
        throw(QueryException(error_message))
    end
    return QueryResult(handle)
end

# explicitly close prepared statement
function DBInterface.close!(stmt::Stmt)
    return _close_stmt(stmt)
end

function execute(con::Connection, sql::AbstractString, params::DBInterface.StatementParams)
    stmt = Stmt(con, sql)
    try
        return execute(stmt, params)
    finally
        _close_stmt(stmt) # immediately close, don't wait for GC
    end
end

execute(con::Connection, sql::AbstractString; kwargs...) = execute(con, sql, values(kwargs))
execute(db::DB, sql::AbstractString, params::DBInterface.StatementParams) = execute(db.main_connection, sql, params)
execute(db::DB, sql::AbstractString; kwargs...) = execute(db.main_connection, sql, values(kwargs))

Tables.isrowtable(::Type{QueryResult}) = true
Tables.columnnames(q::QueryResult) = Tables.columnnames(q.df)

function Tables.schema(q::QueryResult)
	return Tables.schema(q.df)
end

Base.IteratorSize(::Type{QueryResult}) = Base.SizeUnknown()
Base.eltype(q::QueryResult) = Any

function DBInterface.close!(q::QueryResult)
    return _close_result(q)
end

function Base.iterate(q::QueryResult)
    return Base.iterate(eachrow(q.df))
end

function Base.iterate(q::QueryResult, state)
    return Base.iterate(eachrow(q.df), state)
end

DataFrames.DataFrame(q::QueryResult) = DataFrame(q.df)

"Return the last row insert id from the executed statement"
function DBInterface.lastrowid(con::Connection)
    throw(NotImplementedException("Unimplemented: lastrowid"))
end

DBInterface.lastrowid(db::DB) = DBInterface.lastrowid(db.main_connection)

"""
    DBInterface.prepare(db::DuckDB.DB, sql::AbstractString)

Prepare an SQL statement given as a string in the DuckDB database; returns a `DuckDB.Stmt` object.
See `DBInterface.execute`(@ref) for information on executing a prepared statement and passing parameters to bind.
A `DuckDB.Stmt` object can be closed (resources freed) using `DBInterface.close!`(@ref).
"""
DBInterface.prepare(con::Connection, sql::AbstractString) = Stmt(con, sql)
DBInterface.prepare(db::DB, sql::AbstractString) = DBInterface.prepare(db.main_connection, sql)

"""
    DBInterface.execute(db::DuckDB.DB, sql::String, [params])
    DBInterface.execute(stmt::SQLite.Stmt, [params])

Bind any positional (`params` as `Vector` or `Tuple`) or named (`params` as `NamedTuple` or `Dict`) parameters to an SQL statement, given by `db` and `sql` or
as an already prepared statement `stmt`, execute the query and return an iterator of result rows.

Note that the returned result row iterator only supports a single-pass, forward-only iteration of the result rows.
Calling `SQLite.reset!(result)` will re-execute the query and reset the iterator back to the beginning.

The resultset iterator supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, so results can be collected in any Tables.jl-compatible sink,
like `DataFrame(results)`, `CSV.write("results.csv", results)`, etc.
"""
function DBInterface.execute(stmt::Stmt, params::DBInterface.StatementParams)
    return execute(stmt, params)
end

function DBInterface.execute(con::Connection, sql::AbstractString)
    return execute(Stmt(con, sql))
end

Base.show(io::IO, result::DuckDB.QueryResult) = print(io, result.df)
