# test_config.jl

@testset "Test configuration parameters" begin
    # by default NULLs come first
    con = DBInterface.connect(DuckDB.DB, ":memory:")

    results = DBInterface.execute(con, "SELECT 42 a UNION ALL SELECT NULL ORDER BY a")
    df = DataFrame(results)
    @test names(df) == ["a"]
    @test size(df, 1) == 2
    @test isequal(df.a, [missing, 42])

    DBInterface.close!(con)

    # if we add this configuration flag, nulls should come last
    config = DuckDB.Config()
    DuckDB.set_config(config, "default_null_order", "nulls_last")
    @test_throws DuckDB.QueryException DuckDB.set_config(config, "unrecognized option", "aaa")

    con = DBInterface.connect(DuckDB.DB, ":memory:", config)

    # NULL should come last now
    results = DBInterface.execute(con, "SELECT 42 a UNION ALL SELECT NULL ORDER BY a")
    df = DataFrame(results)
    @test names(df) == ["a"]
    @test size(df, 1) == 2
    @test isequal(df.a, [42, missing])

    DBInterface.close!(config)
    DBInterface.close!(config)
    DBInterface.close!(con)
end

@testset "Test Set TimeZone" begin
    con = DBInterface.connect(DuckDB.DB, ":memory:")

    DBInterface.execute(con, "SET TimeZone='UTC'")
    results = DBInterface.execute(con, "SELECT CURRENT_SETTING('TimeZone') AS tz")
    @test isequal(results.df[1, "tz"], "UTC")

    DBInterface.execute(con, "SET TimeZone='America/Los_Angeles'")
    results = DBInterface.execute(con, "SELECT CURRENT_SETTING('TimeZone') AS tz")
    @test isequal(results.df[1, "tz"], "America/Los_Angeles")

    DBInterface.close!(con)
end
