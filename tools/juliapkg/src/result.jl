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

function convert_string(val::duckdb_string_t)
	length = val.length;
	if length <= STRING_INLINE_LENGTH
		return String(collect(val.data[1:length]))
	else
		array = reinterpret(Ptr{UInt8}, collect(val.data[5:12]))[1]
		return unsafe_string(array, length)
	end
end

function convert_date(val::Int32)::Date
	return Dates.epochdays2date(val + 719528)
end

function convert_time(val::Int64)::Time
	return Dates.Time(Dates.Nanosecond(val * 1000))
end

function convert_timestamp(val::Int64)::DateTime
	return Dates.epochms2datetime((val / 1000) + 62167219200000)
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

function convert_column_loop(chunks::Vector{DataChunk}, col_idx::Int64, convert_func::Function, ::Type{SRC}, ::Type{DST}) where {SRC, DST}
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
			size = GetSize(chunk)
			array = GetArray(chunk, col_idx, SRC)
			all_valid = AllValid(chunk, col_idx)
			if !all_valid
				validity = GetValidity(chunk, col_idx)
			end
			for i in 1:size
				if all_valid || IsValid(validity, i)
					result[position] = convert_func(array[i])
				end
				position += 1
			end
		end
	else
		# no missing values
		result = Array{DST}(undef, row_count)
		position = 1
		for chunk in chunks
			size = GetSize(chunk)
			array = GetArray(chunk, col_idx, SRC)
			for i in 1:size
				result[position] = convert_func(array[i])
				position += 1
			end
		end
	end
	return result
end

function standard_convert(chunks::Vector{DataChunk}, col_idx::Int64, ::Type{T}) where {T}
	return convert_column_loop(chunks, col_idx, nop_convert, T, T)
end

function convert_column(chunks::Vector{DataChunk}, col_idx::Int64, type::LogicalType)
	type = GetInternalType(type)
	internal_type = duckdb_type_to_internal_type(type)

	if type == DUCKDB_TYPE_VARCHAR
		return convert_column_loop(chunks, col_idx, convert_string, duckdb_string_t, AbstractString)
	elseif type == DUCKDB_TYPE_DATE
		return convert_column_loop(chunks, col_idx, convert_date, internal_type, Date)
	elseif type == DUCKDB_TYPE_TIME
		return convert_column_loop(chunks, col_idx, convert_time, internal_type, Time)
	elseif type == DUCKDB_TYPE_TIMESTAMP
		return convert_column_loop(chunks, col_idx, convert_timestamp, internal_type, DateTime)
	elseif type == DUCKDB_TYPE_INTERVAL
		return convert_column_loop(chunks, col_idx, convert_interval, internal_type, Dates.CompoundPeriod)
	elseif type == DUCKDB_TYPE_HUGEINT
		return convert_column_loop(chunks, col_idx, convert_hugeint, internal_type, Int128)
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

#
#         type = duckdb_column_type(result, i)
#         internal_type = duckdb_type_to_internal_type(type)
#
#         data = unsafe_wrap(Array, Ptr{internal_type}(duckdb_column_data(result, i)), row_count)
#         nullmask = unsafe_wrap(Array, Ptr{Bool}(duckdb_nullmask_data(result, i)), row_count)
#         bmask = reinterpret(Bool, nullmask)
#
#         if 0 != sum(nullmask)
#             data = data[.!bmask]
#         end
#
#         # Format the different datatypes for DataFrame
#         if type == DUCKDB_TYPE_BOOLEAN
#             data = Bool.(data)
#         elseif type == DUCKDB_TYPE_DATE
#             data = Dates.epochdays2date.(data .+ 719528)
#         elseif type == DUCKDB_TYPE_TIME
#             data = Dates.Time.(Dates.Nanosecond.(data .* 1000))
#         elseif type == DUCKDB_TYPE_TIMESTAMP
#             data = Dates.epochms2datetime.((data ./ 1000) .+ 62167219200000)
#         elseif type == DUCKDB_TYPE_INTERVAL
#             data = map(
#                 x -> Dates.CompoundPeriod(
#                     Dates.Month(x.months),
#                     Dates.Day(x.days),
#                     Dates.Microsecond(x.micros),
#                 ),
#                 data,
#             )
#         elseif type == DUCKDB_TYPE_HUGEINT
#             data = map(x -> x.upper < 1 ? (x.lower::UInt64)%Int64 : x, data)
#         elseif type == DUCKDB_TYPE_VARCHAR
#             data = unsafe_string.(data)
#         end
#
#         if 0 != sum(nullmask)
#             fulldata = Array{Union{Missing,eltype(data)}}(missing, row_count)
#             fulldata[.!bmask] = data
#             data = fulldata
#         end

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
