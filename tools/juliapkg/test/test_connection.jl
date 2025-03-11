# test_connection.jl

@testset "Test opening and closing an in-memory database" begin
    con = DBInterface.connect(DuckDB.DB, ":memory:")
    DBInterface.close!(con)
    # verify that double-closing does not cause any problems
    DBInterface.close!(con)
    DBInterface.close!(con)
    @test 1 == 1

    con = DBInterface.connect(DuckDB.DB, ":memory:")
    @test isopen(con)
    close(con)
    @test !isopen(con)
end

@testset "Test opening a bogus directory" begin
    @test_throws DuckDB.ConnectionException DBInterface.connect(DuckDB.DB, "/path/to/bogus/directory")
end


@testset "Test opening and closing an on-disk database" begin
    # This checks for an issue where the DB and the connection are 
    # closed but the actual db is not (and subsequently cannot be opened
    # in a different process). To check this, we create a DB, write some
    # data to it, close the connection and check if the WAL file exists.
    #
    # Ideally, the WAL file should not exist, but Garbage Collection of Julia
    # may not have run yet, so open database handles may still exist, preventing
    # the database from being closed properly.

    db_path = joinpath(mktempdir(), "duckdata.db")
    db_path_wal = db_path * ".wal"

    function write_data(dbfile::String)
        db = DuckDB.DB(dbfile)
        conn = DBInterface.connect(db)
        DBInterface.execute(conn, "CREATE OR REPLACE TABLE test (a INTEGER, b INTEGER);")
        DBInterface.execute(conn, "INSERT INTO test VALUES (1, 2);")
        DBInterface.close!(conn)
        DuckDB.close_database(db)
        return true
    end
    write_data(db_path) # call the function
    @test isfile(db_path_wal) === false # WAL file should not exist

    @test isfile(db_path) # check if the database file exists

    # check if the database can be opened
    if haskey(ENV, "JULIA_DUCKDB_LIBRARY")
        duckdb_binary = joinpath(dirname(ENV["JULIA_DUCKDB_LIBRARY"]), "..", "duckdb")
        result = run(`$duckdb_binary $db_path -c "SELECT * FROM test LIMIT 1"`) # check if the database can be opened
        @test success(result)
    end
end
