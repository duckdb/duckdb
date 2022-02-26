# using Tables
# using Base.Libc
#
# sym(ptr) = ccall(:jl_symbol, Ref{Symbol}, (Ptr{UInt8},), ptr)
#
# struct Query
#     stmt::Stmt
#     status::Base.RefValue{Cint}
#     names::Vector{Symbol}
#     types::Vector{Type}
#     lookup::Dict{Symbol, Int}
#     current_rownumber::Base.RefValue{Int}
# end
#
# # check if the query has no (more) rows
# Base.isempty(q::Query) = q.status[] == SQLITE_DONE
#
# struct Row <: Tables.AbstractRow
#     q::Query
#     rownumber::Int
# end
#
# getquery(r::Row) = getfield(r, :q)
#
# Tables.isrowtable(::Type{Query}) = true
# Tables.columnnames(q::Query) = q.names
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
# function Tables.schema(q::Query)
#     if isempty(q)
#         # when the query is empty, return the types provided by SQLite
#         # by default SQLite.jl assumes all columns can have missing values
#         Tables.Schema(Tables.columnnames(q), q.types)
#     else
#         return nothing # fallback to the actual column types of the result
#     end
# end
#
# Base.IteratorSize(::Type{Query}) = Base.SizeUnknown()
# Base.eltype(q::Query) = Row
#
# function reset!(q::Query)
#     sqlite3_reset(_stmt(q.stmt).handle)
#     q.status[] = execute(q.stmt)
#     return
# end
#
# function DBInterface.close!(q::Query)
#     _st = _stmt_safe(q.stmt)
#     (_st !== nothing) && sqlite3_reset(_st.handle)
# end
#
# function done(q::Query)
#     st = q.status[]
#     if st == SQLITE_DONE
#         sqlite3_reset(_stmt(q.stmt).handle)
#         return true
#     end
#     st == SQLITE_ROW || sqliteerror(q.stmt.db)
#     return false
# end
#
# @noinline wrongrow(i) = throw(ArgumentError("row $i is no longer valid; sqlite query results are forward-only iterators where each row is only valid when iterated; re-execute the query, convert rows to NamedTuples, or stream the results to a sink to save results"))
#
# function getvalue(q::Query, col::Int, rownumber::Int, ::Type{T}) where {T}
#     rownumber == q.current_rownumber[] || wrongrow(rownumber)
#     handle = _stmt(q.stmt).handle
#     t = sqlite3_column_type(handle, col)
#     if t == SQLITE_NULL
#         return missing
#     else
#         TT = juliatype(t) # native SQLite Int, Float, and Text types
#         return sqlitevalue(ifelse(TT === Any && !isbitstype(T), T, TT), handle, col)
#     end
# end
#
# Tables.getcolumn(r::Row, ::Type{T}, i::Int, nm::Symbol) where {T} = getvalue(getquery(r), i, getfield(r, :rownumber), T)
#
# Tables.getcolumn(r::Row, i::Int) = Tables.getcolumn(r, getquery(r).types[i], i, getquery(r).names[i])
# Tables.getcolumn(r::Row, nm::Symbol) = Tables.getcolumn(r, getquery(r).lookup[nm])
# Tables.columnnames(r::Row) = Tables.columnnames(getquery(r))
#
# function Base.iterate(q::Query)
#     done(q) && return nothing
#     q.current_rownumber[] = 1
#     return Row(q, 1), 2
# end
#
# function Base.iterate(q::Query, rownumber)
#     q.status[] = sqlite3_step(_stmt(q.stmt).handle)
#     done(q) && return nothing
#     q.current_rownumber[] = rownumber
#     return Row(q, rownumber), rownumber + 1
# end
#
# "Return the last row insert id from the executed statement"
# DBInterface.lastrowid(q::Query) = last_insert_rowid(q.stmt.db)
#
# """
#     DBInterface.prepare(db::SQLite.DB, sql::AbstractString)
#
# Prepare an SQL statement given as a string in the sqlite database; returns an `SQLite.Stmt` compiled object.
# See `DBInterface.execute`(@ref) for information on executing a prepared statement and passing parameters to bind.
# A `SQLite.Stmt` object can be closed (resources freed) using `DBInterface.close!`(@ref).
# """
DBInterface.prepare(db::DB, sql::AbstractString) = Stmt(db, sql)
DBInterface.prepare(con::Connection, sql::AbstractString) = Stmt(con, sql)
#
# """
#     DBInterface.execute(db::SQLite.DB, sql::String, [params])
#     DBInterface.execute(stmt::SQLite.Stmt, [params])
#
# Bind any positional (`params` as `Vector` or `Tuple`) or named (`params` as `NamedTuple` or `Dict`) parameters to an SQL statement, given by `db` and `sql` or
# as an already prepared statement `stmt`, execute the query and return an iterator of result rows.
#
# Note that the returned result row iterator only supports a single-pass, forward-only iteration of the result rows.
# Calling `SQLite.reset!(result)` will re-execute the query and reset the iterator back to the beginning.
#
# The resultset iterator supports the [Tables.jl](https://github.com/JuliaData/Tables.jl) interface, so results can be collected in any Tables.jl-compatible sink,
# like `DataFrame(results)`, `CSV.write("results.csv", results)`, etc.
# """
function DBInterface.execute(stmt::Stmt, params::DBInterface.StatementParams)
    status = execute(stmt, params)
    _st = _stmt(stmt)
    cols = sqlite3_column_count(_st.handle)
    header = Vector{Symbol}(undef, cols)
    types = Vector{Type}(undef, cols)
    for i = 1:cols
        nm = sym(sqlite3_column_name(_st.handle, i))
        header[i] = nm
        types[i] = Union{juliatype(_st.handle, i), Missing}
    end
    return Query(stmt, Ref(status), header, types, Dict(x=>i for (i, x) in enumerate(header)), Ref(0))
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
