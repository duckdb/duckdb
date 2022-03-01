using Tables

mutable struct QueryResult
    handle::Ref{duckdb_result}
    names::Vector{Symbol}
    types::Vector{Type}
    lookup::Dict{Symbol, Int}
    total_rows::Int

    function QueryResult(stmt::Stmt, params::DBInterface.StatementParams = ())
        BindParameters(stmt, params)

        handle = Ref{duckdb_result}()
        if duckdb_execute_prepared(stmt.handle, handle) != DuckDBSuccess
            error_message = unsafe_string(duckdb_result_error(handle))
            duckdb_destroy_result(handle)
            throw(QueryException(error_message))
        end

        column_count = duckdb_column_count(handle)
        row_count = duckdb_row_count(handle)
        names = Vector{Symbol}(undef, column_count)
        types = Vector{Type}(undef, column_count)
        for i in 1:column_count
            name = sym(duckdb_column_name(handle, i))
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
            types[i] = duckdb_type_to_julia_type(duckdb_column_type(handle, i))
        end
        lookup = Dict(x => i for (i, x) in enumerate(names))
        result = new(handle, names, types, lookup, row_count)
        finalizer(_close_result, result)
        return result
    end
end

function _close_result(result::QueryResult)
    return duckdb_destroy_result(result.handle)
end

function execute(stmt::Stmt, params::DBInterface.StatementParams = ())
    return QueryResult(stmt, params)
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

struct Row <: Tables.AbstractRow
    q::QueryResult
    row_number::Int
end

getquery(r::Row) = getfield(r, :q)

Tables.isrowtable(::Type{QueryResult}) = true
Tables.columnnames(q::QueryResult) = q.names

function Tables.schema(q::QueryResult)
    if isempty(q)
        # when the query is empty, return the types provided by SQLite
        # by default SQLite.jl assumes all columns can have missing values
        Tables.Schema(Tables.columnnames(q), q.types)
    else
        return nothing # fallback to the actual column types of the result
    end
end

Base.IteratorSize(::Type{QueryResult}) = Base.SizeUnknown()
Base.eltype(q::QueryResult) = Row

function DBInterface.close!(q::QueryResult)
    return _close_result(q)
end

duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Bool} =
    duckdb_value_boolean(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Int8} =
    duckdb_value_int8(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Int16} =
    duckdb_value_int16(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Int32} =
    duckdb_value_int32(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Int64} =
    duckdb_value_int64(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: UInt8} =
    duckdb_value_uint8(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: UInt16} =
    duckdb_value_uint16(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: UInt32} =
    duckdb_value_uint32(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: UInt64} =
    duckdb_value_uint64(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Float32} =
    duckdb_value_float(handle, col, row_number)
duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T <: Float64} =
    duckdb_value_double(handle, col, row_number)
function duckdb_internal_value(::Type{T}, handle::duckdb_result, col::Int, row_number::Int) where {T}
    return unsafe_string(duckdb_value_varchar(handle, col, row_number))
end

function getvalue(q::QueryResult, col::Int, row_number::Int, ::Type{T}) where {T}
    if duckdb_value_is_null(q.handle[], col, row_number)
        return missing
    end
    return duckdb_internal_value(T, q.handle[], col, row_number)
end

Tables.getcolumn(r::Row, ::Type{T}, i::Int, nm::Symbol) where {T} =
    getvalue(getquery(r), i, getfield(r, :row_number), T)

Tables.getcolumn(r::Row, i::Int) = Tables.getcolumn(r, getquery(r).types[i], i, getquery(r).names[i])
Tables.getcolumn(r::Row, nm::Symbol) = Tables.getcolumn(r, getquery(r).lookup[nm])
Tables.columnnames(r::Row) = Tables.columnnames(getquery(r))

function Base.iterate(q::QueryResult)
    if q.total_rows == 0
        return nothing
    end
    return Row(q, 1), 2
end

function Base.iterate(q::QueryResult, row_number)
    if row_number > q.total_rows
        return nothing
    end
    return Row(q, row_number), row_number + 1
end

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
