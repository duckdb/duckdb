# # prepare given sql statement
# function _Stmt(db::DB, sql::AbstractString)
#     handle = Ref{duckdb_prepared_statement}()
#     sqliteprepare(db, sql, handle, Ref{duckdb_prepared_statement}())
#     return _Stmt(handle[])
# end
#
# """
#     SQLite.Stmt(db, sql) => SQL.Stmt
#
# Prepares an optimized internal representation of SQL statement in
# the context of the provided SQLite3 `db` and constructs the `SQLite.Stmt`
# Julia object that holds a reference to the prepared statement.
#
# *Note*: the `sql` statement is not actually executed, but only compiled
# (mainly for usage where the same statement is executed multiple times
# with different parameters bound as values).
#
# Internally `SQLite.Stmt` constructor creates the [`SQLite._Stmt`](@ref) object that is managed by `db`.
# `SQLite.Stmt` references the `SQLite._Stmt` by its unique id.
#
# The `SQLite.Stmt` will be automatically closed/shutdown when it goes out of scope
# (i.e. the end of the Julia session, end of a function call wherein it was created, etc.).
# One can also call `DBInterface.close!(stmt)` to immediately close it.
#
# All prepared statements of a given DB connection are also automatically closed when the
# DB is disconnected or when [`SQLite.finalize_statements!`](@ref) is explicitly called.
# """
mutable struct Stmt <: DBInterface.Statement
    con::Connection
    handle::duckdb_prepared_statement

    function Stmt(con::Connection, sql::AbstractString)
        handle = Ref{duckdb_prepared_statement}()
        if duckdb_prepare(con.handle, sql, handle) != DuckDBSuccess
            error_message = unsafe_string(duckdb_prepare_error(handle))
            duckdb_destroy_prepare(handle)
            throw(QueryException(error_message))
        end
        stmt = new(con, handle[])
        finalizer(_close_stmt, stmt)
        return stmt
    end

    function Stmt(db::DB, sql::AbstractString)
        return Stmt(db.main_connection, sql)
    end
end

function _close_stmt(stmt::Stmt)
    if stmt.handle != C_NULL
        duckdb_destroy_prepare(stmt.handle)
        stmt.handle = C_NULL
    end
end

DBInterface.getconnection(stmt::Stmt) = stmt.con

# function bind! end
#
# function bind!(stmt::_Stmt, params::DBInterface.NamedStatementParams)
#     nparams = sqlite3_bind_parameter_count(stmt.handle)
#     (nparams <= length(params)) || throw(SQLiteException("values should be provided for all query placeholders"))
#     for i in 1:nparams
#         name = unsafe_string(sqlite3_bind_parameter_name(stmt.handle, i))
#         isempty(name) && throw(SQLiteException("nameless parameters should be passed as a Vector"))
#         # name is returned with the ':', '@' or '$' at the start
#         sym = Symbol(name[2:end])
#         haskey(params, sym) || throw(SQLiteException("`$name` not found in values keyword arguments to bind to sql statement"))
#         bind!(stmt, i, params[sym])
#     end
# end

# function bind!(stmt::_Stmt, values::DBInterface.PositionalStatementParams)
#     nparams = sqlite3_bind_parameter_count(stmt.handle)
#     (nparams == length(values)) || throw(SQLiteException("values should be provided for all query placeholders"))
#     for i in 1:nparams
#         @inbounds bind!(stmt, i, values[i])
#     end
# end

# bind!(stmt::Stmt, values::DBInterface.StatementParams) = bind!(_stmt(stmt), values)
#
# bind!(stmt::Union{_Stmt, Stmt}; kwargs...) = bind!(stmt, kwargs.data)

# Binding parameters to SQL statements
# function bind!(stmt::_Stmt, name::AbstractString, val::Any)
#     i::Int = sqlite3_bind_parameter_index(stmt.handle, name)
#     if i == 0
#         throw(SQLiteException("SQL parameter $name not found in $stmt"))
#     end
#     return bind!(stmt, i, val)
# end

duckdb_bind_internal(stmt::Stmt, i::Integer, val::AbstractFloat) = duckdb_bind_double(stmt.handle, i, Float64(val));
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Bool) = duckdb_bind_boolean(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Int8) = duckdb_bind_int8(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Int16) = duckdb_bind_int16(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Int32) = duckdb_bind_int32(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Int64) = duckdb_bind_int64(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::UInt8) = duckdb_bind_uint8(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::UInt16) = duckdb_bind_uint16(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::UInt32) = duckdb_bind_uint32(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::UInt64) = duckdb_bind_uint64(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Float32) = duckdb_bind_float(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Float64) = duckdb_bind_double(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Missing) = duckdb_bind_null(stmt.handle, i);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Nothing) = duckdb_bind_null(stmt.handle, i);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::AbstractString) = duckdb_bind_varchar(stmt.handle, i, val);
duckdb_bind_internal(stmt::Stmt, i::Integer, val::Vector{UInt8})  = duckdb_bind_blob(stmt.handle, i, val, sizeof(val));
duckdb_bind_internal(stmt::Stmt, i::Integer, val::WeakRefString{UInt8}) =
    duckdb_bind_varchar(stmt.handle, i, val.ptr, val.len);

function duckdb_bind_internal(stmt::Stmt, i::Integer, val::Any)
	println(val);
    throw(NotImplementedException("unsupported type for bind"))
    # bind!(stmt, i, sqlserialize(val))
end

function BindParameters(stmt::Stmt, params::DBInterface.StatementParams)
    i = 1
    for param in params
        if duckdb_bind_internal(stmt, i, param) != DuckDBSuccess
            throw(QueryException("Failed to bind parameter"))
        end
        i += 1
    end
end
