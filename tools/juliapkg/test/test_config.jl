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
