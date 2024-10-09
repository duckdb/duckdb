# test_basic_queries.jl

using Tables: partitions

@testset "Test DBInterface.execute" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(con, "SELECT 42 a")

    # iterator
    for row in Tables.rows(results)
        @test row.a == 42
        @test row[1] == 42
    end

    # convert to DataFrame
    df = DataFrame(results)
    @test names(df) == ["a"]
    @test size(df, 1) == 1
    @test df.a == [42]

    # do block syntax to automatically close cursor
    df = DBInterface.execute(con, "SELECT 42 a") do results
        return DataFrame(results)
    end
    @test names(df) == ["a"]
    @test size(df, 1) == 1
    @test df.a == [42]

    DBInterface.close!(con)
end

@testset "Test numeric data types" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(
        con,
        """
SELECT 42::TINYINT a, 42::INT16 b, 42::INT32 c, 42::INT64 d, 42::UINT8 e, 42::UINT16 f, 42::UINT32 g, 42::UINT64 h
UNION ALL
SELECT NULL, NULL, NULL, NULL, NULL, NULL, 43, NULL
"""
    )

    df = DataFrame(results)

    @test size(df, 1) == 2
    @test isequal(df.a, [42, missing])
    @test isequal(df.b, [42, missing])
    @test isequal(df.c, [42, missing])
    @test isequal(df.d, [42, missing])
    @test isequal(df.e, [42, missing])
    @test isequal(df.f, [42, missing])
    @test isequal(df.g::Vector{Int}, [42, 43])
    @test isequal(df.h, [42, missing])

    DBInterface.close!(con)
end

@testset "Test strings" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(
        con,
        """
SELECT 'hello world' s
UNION ALL
SELECT NULL
UNION ALL
SELECT 'this is a long string'
UNION ALL
SELECT 'obligatory mühleisen'
UNION ALL
SELECT '🦆🍞🦆'
"""
    )

    df = DataFrame(results)
    @test size(df, 1) == 5
    @test isequal(df.s, ["hello world", missing, "this is a long string", "obligatory mühleisen", "🦆🍞🦆"])

    for s in ["foo", "🦆DB", SubString("foobar", 1, 3), SubString("🦆ling", 1, 6)]
        results = DBInterface.execute(con, "SELECT length(?) as len", [s])
        @test only(results).len == 3
    end

    DBInterface.close!(con)
end

@testset "DBInterface.execute - parser error" begin
    con = DBInterface.connect(DuckDB.DB)

    # parser error
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELEC")

    DBInterface.close!(con)
end

@testset "DBInterface.execute - binder error" begin
    con = DBInterface.connect(DuckDB.DB)

    # binder error
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM this_table_does_not_exist")

    DBInterface.close!(con)
end

@testset "DBInterface.execute - runtime error" begin
    con = DBInterface.connect(DuckDB.DB)

    res = DBInterface.execute(con, "select current_setting('threads')")
    df = DataFrame(res)
    print(df)

    # run-time error
    @test_throws DuckDB.QueryException DBInterface.execute(
        con,
        "SELECT i::int FROM (SELECT '42' UNION ALL SELECT 'hello') tbl(i)"
    )

    DBInterface.close!(con)
end

# test a PIVOT query that generates multiple prepared statements and will fail with execute
@testset "Test DBInterface.query" begin
    db = DuckDB.DB()
    con = DuckDB.connect(db)
    DuckDB.execute(con, "CREATE TABLE Cities (Country VARCHAR, Name VARCHAR, Year INT, Population INT);")
    DuckDB.execute(con, "INSERT INTO Cities VALUES ('NL', 'Amsterdam', 2000, 1005)")
    DuckDB.execute(con, "INSERT INTO Cities VALUES ('NL', 'Amsterdam', 2010, 1065)")
    results = DuckDB.query(con, "PIVOT Cities ON Year USING first(Population);")

    # iterator
    for row in Tables.rows(results)
        @test row[:Name] == "Amsterdam"
        @test row[4] == 1065
    end

    # convert to DataFrame
    df = DataFrame(results)
    @test names(df) == ["Country", "Name", "2000", "2010"]
    @test size(df, 1) == 1
    @test df[1, :Country] == "NL"
    @test df[1, :Name] == "Amsterdam"
    @test df[1, "2000"] == 1005
    @test df[1, 4] == 1065

    @test DataFrame(DuckDB.query(db, "select 'a'; select 2;"))[1, 1] == "a"

    DBInterface.close!(con)
end

@testset "Test chunked response" begin
    con = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(con, "CREATE TABLE chunked_table AS SELECT * FROM range(2049)")
    result = DBInterface.execute(con, "SELECT * FROM chunked_table;")
    chunks_it = partitions(result)
    chunks = collect(chunks_it)
    @test length(chunks) == 2
    @test_throws DuckDB.NotImplementedException collect(chunks_it)

    result = DBInterface.execute(con, "SELECT * FROM chunked_table;", DuckDB.StreamResult)
    chunks_it = partitions(result)
    chunks = collect(chunks_it)
    @test length(chunks) == 2
    @test_throws DuckDB.NotImplementedException collect(chunks_it)

    DuckDB.execute(
        con,
        """
CREATE TABLE large (x1 INT, x2 INT, x3 INT, x4 INT, x5 INT, x6 INT, x7 INT, x8 INT, x9 INT, x10 INT, x11 INT);
"""
    )
    DuckDB.execute(con, "INSERT INTO large VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);")
    result = DBInterface.execute(con, "SELECT * FROM large ;")
    chunks_it = partitions(result)
    chunks = collect(chunks_it)
    @test length(chunks) == 1

    DBInterface.close!(con)
end
