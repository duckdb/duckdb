using Tables

mutable struct QueryResult
    handle::duckdb_result
    names::Vector{Symbol}
    types::Vector{Type}
    lookup::Dict{Symbol, Int}
    total_rows::Int

    function QueryResult(stmt::Stmt, params::DBInterface.StatementParams = ())
        handle = Ref{duckdb_result}()
        if duckdb_execute_prepared(stmt.handle, handle) != DuckDBSuccess
            error_message = unsafe_string(duckdb_result_error(handle))
            duckdb_destroy_result(handle)
            throw(error_message)
        end

        column_count = duckdb_column_count(handle)
        row_count = duckdb_row_count(handle)
        names = Vector{Symbol}(undef, column_count)
        types = Vector{Type}(undef, column_count)
        for i in 1:column_count
            nm = sym(duckdb_column_name(handle, i))
            names[i] = nm
            types[i] = duckdb_type_to_julia_type(duckdb_column_type(handle, i))
        end
        lookup = Dict(x => i for (i, x) in enumerate(names))
        result = new(handle[], names, types, lookup, row_count)
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

DBInterface.getconnection(stmt::Stmt) = stmt.db

# explicitly close prepared statement
function DBInterface.close!(stmt::Stmt)
    return _close_stmt(stmt)
end

#
# sqliteprepare(db::DB, sql::AbstractString, stmt::Ref{duckdb_prepared_statement}, null::Ref{duckdb_prepared_statement}) =
#     @CHECK db duckdb_prepare(db.handle, sql, stmt)
#
# # include("UDF.jl")
# export @sr_str
#
# """
#     SQLite.clear!(stmt::SQLite.Stmt)
#
# Clears any bound values to a prepared SQL statement
# """
# function clear!(stmt::Stmt)
#     _st = _stmt(stmt)
#     sqlite3_clear_bindings(_st.handle)
#     empty!(_st.params)
#     return
# end
#
# """
#     SQLite.bind!(stmt::SQLite.Stmt, values)
#
# bind `values` to parameters in a prepared [`SQLite.Stmt`](@ref). Values can be:
#
# * `Vector` or `Tuple`: where each element will be bound to an SQL parameter by index order
# * `Dict` or `NamedTuple`; where values will be bound to named SQL parameters by the `Dict`/`NamedTuple` key
#
# Additional methods exist for working individual SQL parameters:
#
# * `SQLite.bind!(stmt, name, val)`: bind a single value to a named SQL parameter
# * `SQLite.bind!(stmt, index, val)`: bind a single value to a SQL parameter by index number
#
# From the [SQLite documentation](https://www3.sqlite.org/cintro.html):
#
# > Usually, though,
# > it is not useful to evaluate exactly the same SQL statement more than once.
# > More often, one wants to evaluate similar statements.
# > For example, you might want to evaluate an INSERT statement
# > multiple times though with different values to insert.
# > To accommodate this kind of flexibility,
# > SQLite allows SQL statements to contain parameters
# > which are "bound" to values prior to being evaluated.
# > These values can later be changed and the same prepared statement
# > can be evaluated a second time using the new values.
# >
# > In SQLite,
# > wherever it is valid to include a string literal,
# > one can use a parameter in one of the following forms:
# >
# > - `?`
# > - `?NNN`
# > - `:AAA`
# > - `\$AAA`
# > - `@AAA`
# >
# > In the examples above,
# > `NNN` is an integer value and `AAA` is an identifier.
# > A parameter initially has a value of `NULL`.
# > Prior to calling `sqlite3_step()` for the first time
# > or immediately after `sqlite3_reset()``,
# > the application can invoke one of the `sqlite3_bind()` interfaces
# > to attach values to the parameters.
# > Each call to `sqlite3_bind()` overrides prior bindings on the same parameter.
#
# """
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
#
# function bind!(stmt::_Stmt, values::DBInterface.PositionalStatementParams)
#     nparams = sqlite3_bind_parameter_count(stmt.handle)
#     (nparams == length(values)) || throw(SQLiteException("values should be provided for all query placeholders"))
#     for i in 1:nparams
#         @inbounds bind!(stmt, i, values[i])
#     end
# end
#
# bind!(stmt::Stmt, values::DBInterface.StatementParams) = bind!(_stmt(stmt), values)
#
# bind!(stmt::Union{_Stmt, Stmt}; kwargs...) = bind!(stmt, kwargs.data)
#
# # Binding parameters to SQL statements
# function bind!(stmt::_Stmt, name::AbstractString, val::Any)
#     i::Int = sqlite3_bind_parameter_index(stmt.handle, name)
#     if i == 0
#         throw(SQLiteException("SQL parameter $name not found in $stmt"))
#     end
#     return bind!(stmt, i, val)
# end
#
# # binding method for internal _Stmt class
# bind!(stmt::_Stmt, i::Integer, val::AbstractFloat)  = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_double(stmt.handle, i, Float64(val)); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Int32)          = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_int(stmt.handle, i, val); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Int64)          = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_int64(stmt.handle, i, val); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Missing)        = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_null(stmt.handle, i); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Nothing)        = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_null(stmt.handle, i); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::AbstractString) = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_text(stmt.handle, i, val); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::WeakRefString{UInt8})   = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_text(stmt.handle, i, val.ptr, val.len); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::WeakRefString{UInt16})  = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_text16(stmt.handle, i, val.ptr, val.len*2); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Bool)           = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_int(stmt.handle, i, Int32(val)); return nothing)
# bind!(stmt::_Stmt, i::Integer, val::Vector{UInt8})  = (stmt.params[i] = val; @CHECK stmt.db sqlite3_bind_blob(stmt.handle, i, val); return nothing)
# # Fallback is BLOB and defaults to serializing the julia value
#
# bind!(stmt::Stmt, param::Union{Integer, AbstractString}, val::Any) = bind!(_stmt(stmt), param, val)
#
# # internal wrapper mutable struct to, in-effect, mark something which has been serialized
# struct Serialized
#     object
# end
#
# function sqlserialize(x)
#     buffer = IOBuffer()
#     # deserialize will sometimes return a random object when called on an array
#     # which has not been previously serialized, we can use this mutable struct to check
#     # that the array has been serialized
#     s = Serialized(x)
#     Serialization.serialize(buffer, s)
#     return take!(buffer)
# end
# # fallback method to bind arbitrary julia `val` to the parameter at index `i` (object is serialized)
# bind!(stmt::_Stmt, i::Integer, val::Any) = bind!(stmt, i, sqlserialize(val))
#
# struct SerializeError <: Exception
#     msg::String
# end
#
# # magic bytes that indicate that a value is in fact a serialized julia value, instead of just a byte vector
# # these bytes depend on the julia version and other things, so they are determined using an actual serialization
# const SERIALIZATION = sqlserialize(0)[1:18]
#
# function sqldeserialize(r)
#     if sizeof(r) < sizeof(SERIALIZATION)
#         return r
#     end
#     ret = Base.Libc.ccall(:memcmp, Int32, (Ptr{UInt8}, Ptr{UInt8}, UInt),
#             SERIALIZATION, r, min(sizeof(SERIALIZATION), sizeof(r)))
#     if ret == 0
#         try
#             v = Serialization.deserialize(IOBuffer(r))
#             return v.object
#         catch e
#             throw(SerializeError("Error deserializing non-primitive value out of database; this is probably due to using SQLite.jl with a different Julia version than was used to originally serialize the database values. The same Julia version that was used to serialize should be used to extract the database values into a different format (csv file, feather file, etc.) and then loaded back into the sqlite database with the current Julia version."))
#         end
#     else
#         return r
#     end
# end
# #TODO:
#  #int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int n);
#  #int sqlite3_bind_value(sqlite3_stmt*, int, const sqlite3_value*);
#
# sqlitevalue(::Type{T}, handle, col) where {T <: Union{Base.BitSigned, Base.BitUnsigned}} = convert(T, sqlite3_column_int64(handle, col))
# const FLOAT_TYPES = Union{Float16, Float32, Float64} # exclude BigFloat
# sqlitevalue(::Type{T}, handle, col) where {T <: FLOAT_TYPES} = convert(T, sqlite3_column_double(handle, col))
# #TODO: test returning a WeakRefString instead of calling `unsafe_string`
# sqlitevalue(::Type{T}, handle, col) where {T <: AbstractString} = convert(T, unsafe_string(sqlite3_column_text(handle, col)))
# function sqlitevalue(::Type{T}, handle, col) where {T}
#     blob = convert(Ptr{UInt8}, sqlite3_column_blob(handle, col))
#     b = sqlite3_column_bytes(handle, col)
#     buf = zeros(UInt8, b) # global const?
#     unsafe_copyto!(pointer(buf), blob, b)
#     r = sqldeserialize(buf)
#     return r
# end
#
# # conversion from Julia to SQLite3 types
# sqlitetype_(::Type{<:Integer}) = "INT"
# sqlitetype_(::Type{<:AbstractFloat}) = "REAL"
# sqlitetype_(::Type{<:AbstractString}) = "TEXT"
# sqlitetype_(::Type{Bool}) = "INT"
# sqlitetype_(::Type) = "BLOB" # fallback
#
# sqlitetype(::Type{Missing}) = "NULL"
# sqlitetype(::Type{Nothing}) = "NULL"
# sqlitetype(::Type{Union{T, Missing}}) where T = sqlitetype_(T)
# sqlitetype(::Type{T}) where T = string(sqlitetype_(T), " NOT NULL")
#
# """
#     SQLite.execute(db::SQLite.DB, sql::AbstractString, [params]) -> Int
#     SQLite.execute(stmt::SQLite.Stmt, [params]) -> Int
#
# An internal method that executes the SQL statement (provided either as a `db` connection and `sql` command,
# or as an already prepared `stmt` (see [`SQLite.Stmt`](@ref))) with given `params` parameters
# (either positional (`Vector` or `Tuple`), named (`Dict` or `NamedTuple`), or specified as keyword arguments).
#
# Returns the SQLite status code of operation.
#
# *Note*: this is a low-level method that just executes the SQL statement,
# but does not retrieve any data from `db`.
# To get the results of a SQL query, it is recommended to use [`DBInterface.execute`](@ref).
# """
# function execute end
#
# function execute(stmt::Stmt, params::DBInterface.StatementParams=())
# 	throw("execute");
# end
#
# execute(stmt::Stmt, params::DBInterface.StatementParams) =
#     execute(stmt.db, _stmt(stmt), params)
#
# execute(stmt::Stmt; kwargs...) = execute(stmt, values(kwargs))
#
function execute(db::DB, sql::AbstractString, params::DBInterface.StatementParams)
    stmt = Stmt(db, sql)
    try
        return stmt.execute(params)
    finally
        _close_stmt(stmt) # immediately close, don't wait for GC
    end
end

execute(db::DB, sql::AbstractString; kwargs...) = execute(db, sql, values(kwargs))

function DBInterface.transaction(f, db::DB)
    throw("transaction")
end

"""
    DuckDB.commit(db)
    DuckDB.commit(db, name)

commit a transaction or named savepoint
"""
function commit end

commit(db::DB) = execute(db, "COMMIT TRANSACTION;")

"""
    DuckDB.rollback(db)
    DuckDB.rollback(db, name)

rollback transaction or named savepoint
"""
function rollback end

rollback(db::DB) = execute(db, "ROLLBACK TRANSACTION;")

struct Row <: Tables.AbstractRow
    q::QueryResult
    row_number::Int
end

getquery(r::Row) = getfield(r, :q)

Tables.isrowtable(::Type{QueryResult}) = true
Tables.columnnames(q::QueryResult) = q.names
#
# struct DBTable
#     name::String
#     schema::Union{Tables.Schema, Nothing}
# end
#
# DBTable(name::String) = DBTable(name, nothing)
#
# const DBTables = AbstractVector{DBTable}
#
# Tables.istable(::Type{<:DBTables}) = true
# Tables.rowaccess(::Type{<:DBTables}) = true
# Tables.rows(dbtbl::DBTables) = dbtbl
#
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
    if duckdb_value_is_null(q.handle, col, row_number)
        return missing
    end
    return duckdb_internal_value(T, q.handle, col, row_number)
end

#
# duckdb_value_boolean
# duckdb_value_int8
# duckdb_value_int16
# duckdb_value_int32
# duckdb_value_int64
# duckdb_value_uint8
# duckdb_value_uint16
# duckdb_value_uint32
# duckdb_value_uint64
# duckdb_value_float
# duckdb_value_double
# const FLOAT_TYPES = Union{Float16, Float32, Float64} # exclude BigFloat
# sqlitevalue(::Type{T}, handle, col) where {T <: FLOAT_TYPES} = convert(T, sqlite3_column_double(handle, col))

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
function DBInterface.lastrowid(q::QueryResult)
    throw("unimplemented: lastrowid")
    # 	last_insert_rowid(q.stmt.db)
end

"""
    DBInterface.prepare(db::SQLite.DB, sql::AbstractString)

Prepare an SQL statement given as a string in the sqlite database; returns an `SQLite.Stmt` compiled object.
See `DBInterface.execute`(@ref) for information on executing a prepared statement and passing parameters to bind.
A `SQLite.Stmt` object can be closed (resources freed) using `DBInterface.close!`(@ref).
"""
DBInterface.prepare(db::DB, sql::AbstractString) = Stmt(db, sql)
DBInterface.prepare(con::Connection, sql::AbstractString) = Stmt(con, sql)

"""
    DBInterface.execute(db::SQLite.DB, sql::String, [params])
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

#
# """
#     SQLite.createtable!(db::SQLite.DB, table_name, schema::Tables.Schema; temp=false, ifnotexists=true)
#
# Create a table in `db` with name `table_name`, according to `schema`, which is a set of column names and types, constructed like `Tables.Schema(names, types)`
# where `names` can be a vector or tuple of String/Symbol column names, and `types` is a vector or tuple of sqlite-compatible types (`Int`, `Float64`, `String`, or unions of `Missing`).
#
# If `temp=true`, the table will be created temporarily, which means it will be deleted when the `db` is closed.
# If `ifnotexists=true`, no error will be thrown if the table already exists.
# """
# function createtable!(db::DB, name::AbstractString, ::Tables.Schema{names, types};
#                       temp::Bool=false, ifnotexists::Bool=true) where {names, types}
#     temp = temp ? "TEMP" : ""
#     ifnotexists = ifnotexists ? "IF NOT EXISTS" : ""
#     columns = [string(esc_id(String(names[i])), ' ',
#                       sqlitetype(types !== nothing ? fieldtype(types, i) : Any))
#                for i in eachindex(names)]
#     sql = "CREATE $temp TABLE $ifnotexists $(esc_id(string(name))) ($(join(columns, ',')))"
#     return execute(db, sql)
# end
#
# # table info for load!():
# # returns NamedTuple with columns information,
# # or nothing if table does not exist
# tableinfo(db::DB, name::AbstractString) =
#     DBInterface.execute(db, "pragma table_info($(esc_id(name)))") do qry
#         st = qry.status[]
#         if st == SQLITE_ROW
#             return Tables.columntable(qry)
#         elseif st == SQLITE_DONE
#             return nothing
#         else
#             sqliteerror(q.stmt.db)
#         end
#     end
#
# """
#     source |> SQLite.load!(db::SQLite.DB, tablename::String; temp::Bool=false, ifnotexists::Bool=false, replace::Bool=false, analyze::Bool=false)
#     SQLite.load!(source, db, tablename; temp=false, ifnotexists=false, replace::Bool=false, analyze::Bool=false)
#
# Load a Tables.jl input `source` into an SQLite table that will be named `tablename` (will be auto-generated if not specified).
#
#   * `temp=true` will create a temporary SQLite table that will be destroyed automatically when the database is closed
#   * `ifnotexists=false` will throw an error if `tablename` already exists in `db`
#   * `replace=false` controls whether an `INSERT INTO ...` statement is generated or a `REPLACE INTO ...`
#   * `analyze=true` will execute `ANALYZE` at the end of the insert
# """
# function load! end
#
# load!(db::DB, name::AbstractString="sqlitejl_"*Random.randstring(5); kwargs...) =
#     x -> load!(x, db, name; kwargs...)
#
# function load!(itr, db::DB, name::AbstractString="sqlitejl_"*Random.randstring(5); kwargs...)
#     # check if table exists
#     db_tableinfo = tableinfo(db, name)
#     rows = Tables.rows(itr)
#     sch = Tables.schema(rows)
#     return load!(sch, rows, db, name, db_tableinfo; kwargs...)
# end
#
# # case-insensitive check for duplicate column names
# function checkdupnames(names::Union{AbstractVector, Tuple})
#     checkednames = Set{String}()
#     for name in names
#         lcname = lowercase(string(name))
#         if lcname in checkednames
#             throw(SQLiteException("Duplicate case-insensitive column name $lcname detected. SQLite doesn't allow duplicate column names and treats them case insensitive"))
#         end
#         push!(checkednames, lcname)
#     end
#     return true
# end
#
# # check if schema names match column names in DB
# function checknames(::Tables.Schema{names}, db_names::AbstractVector{String}) where {names}
#     table_names = Set(string.(names))
#     db_names = Set(db_names)
#
#     if table_names != db_names
#         throw(SQLiteException("Error loading, column names from table $(collect(table_names)) do not match database names $(collect(db_names))"))
#     end
#     return true
# end
#
# function load!(sch::Tables.Schema, rows, db::DB, name::AbstractString, db_tableinfo::Union{NamedTuple, Nothing}, row=nothing, st=nothing;
#                temp::Bool=false, ifnotexists::Bool=false, replace::Bool=false, analyze::Bool=false)
#     # check for case-insensitive duplicate column names (sqlite doesn't allow)
#     checkdupnames(sch.names)
#     # check if `rows` column names match the existing table, or create the new one
#     if db_tableinfo !== nothing
#         checknames(sch, db_tableinfo.name)
#     else
#         createtable!(db, name, sch; temp=temp, ifnotexists=ifnotexists)
#     end
#     # build insert statement
#     columns = join(esc_id.(string.(sch.names)), ",")
#     params = chop(repeat("?,", length(sch.names)))
#     kind = replace ? "REPLACE" : "INSERT"
#     stmt = _Stmt(db, "$kind INTO $(esc_id(string(name))) ($columns) VALUES ($params)")
#     # start a transaction for inserting rows
#     DBInterface.transaction(db) do
#         if row === nothing
#             state = iterate(rows)
#             state === nothing && return
#             row, st = state
#         end
#         while true
#             Tables.eachcolumn(sch, row) do val, col, _
#                 bind!(stmt, col, val)
#             end
#             r = sqlite3_step(stmt.handle)
#             if r == SQLITE_DONE
#                 sqlite3_reset(stmt.handle)
#             elseif r != SQLITE_ROW
#                 e = sqliteexception(db, stmt)
#                 sqlite3_reset(stmt.handle)
#                 throw(e)
#             end
#             state = iterate(rows, st)
#             state === nothing && break
#             row, st = state
#         end
#     end
#     _close!(stmt)
#     analyze && execute(db, "ANALYZE $name")
#     return name
# end
#
# # unknown schema case
# function load!(::Nothing, rows, db::DB, name::AbstractString,
#                db_tableinfo::Union{NamedTuple, Nothing}; kwargs...)
#     state = iterate(rows)
#     state === nothing && return name
#     row, st = state
#     names = propertynames(row)
#     sch = Tables.Schema(names, nothing)
#     return load!(sch, rows, db, name, db_tableinfo, row, st; kwargs...)
# end
