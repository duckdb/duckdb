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
        if duckdb_prepare(con.handle, sql, handle) != 0
        	throw("failed to open connection")
        end
		stmt = new(con, handle[])
		finalizer(_close_stmt, stmt)
		return stmt
    end

    function Stmt(db::DB, sql::AbstractString)
    	return Stmt(db.main_connection, sql);
    end
end

function _close_stmt(stmt::Stmt)
    if stmt.handle != C_NULL
    	duckdb_destroy_prepare(stmt.handle)
    	stmt.handle = C_NULL
    end
end
