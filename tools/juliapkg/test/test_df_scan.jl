# test_df_scan.jl

@testset "Test standard DataFrame scan" begin
    con = DBInterface.connect(DuckDB.DB)
    df = DataFrame(a = [1, 2, 3], b = [42, 84, 42])

    DuckDB.RegisterDataFrame(con, df, "my_df")
    GC.gc()

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["a", "b"]
    @test size(df, 1) == 3
    @test df.a == [1, 2, 3]
    @test df.b == [42, 84, 42]

    DBInterface.close!(con)
end

@testset "Test DataFrame scan with NULL values" begin
    con = DBInterface.connect(DuckDB.DB)
    df = DataFrame(a = [1, missing, 3], b = [missing, 84, missing])

    DuckDB.RegisterDataFrame(con, df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test names(df) == ["a", "b"]
    @test size(df, 1) == 3
    @test isequal(df.a, [1, missing, 3])
    @test isequal(df.b, [missing, 84, missing])

    DBInterface.close!(con)
end
