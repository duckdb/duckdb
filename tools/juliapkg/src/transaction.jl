
function DBInterface.transaction(f, con::Connection)
    begin_transaction(con)
    try
        f()
    catch
        rollback(con)
        rethrow()
    finally
        commit(con)
    end
end

function DBInterface.transaction(f, db::DB)
    return DBInterface.transaction(f, db.main_connection)
end

"""
    DuckDB.begin(db)

begin a transaction
"""
function begin_transaction end

begin_transaction(con::Connection) = execute(con, "BEGIN TRANSACTION;")
begin_transaction(db::DB) = begin_transaction(db.main_connection)
transaction(con::Connection) = begin_transaction(con)
transaction(db::DB) = begin_transaction(db)

"""
    DuckDB.commit(db)

commit a transaction
"""
function commit end

commit(con::Connection) = execute(con, "COMMIT TRANSACTION;")
commit(db::DB) = commit(db.main_connection)

"""
    DuckDB.rollback(db)

rollback transaction
"""
function rollback end

rollback(con::Connection) = execute(con, "ROLLBACK TRANSACTION;")
rollback(db::DB) = rollback(db.main_connection)
