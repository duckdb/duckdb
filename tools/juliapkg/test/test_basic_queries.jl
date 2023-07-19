# test_basic_queries.jl

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

    DBInterface.close!(con)
end

@testset "Test numeric data types" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(
        con,
        """
SELECT 42::TINYINT a, 42::INT16 b, 42::INT32 c, 42::INT64 d, 42::UINT8 e, 42::UINT16 f, 42::UINT32 g, 42::UINT64 h
UNION ALL
SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
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
    @test isequal(df.g, [42, missing])
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

    DBInterface.close!(con)
end

@testset "DBInterface.execute errors" begin
    con = DBInterface.connect(DuckDB.DB)

    # parser error
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELEC")

    # binder error
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM this_table_does_not_exist")

    # run-time error
    @test_throws DuckDB.QueryException DBInterface.execute(
        con,
        "SELECT i::int FROM (SELECT '42' UNION ALL SELECT 'hello') tbl(i)"
    )

    DBInterface.close!(con)
end
