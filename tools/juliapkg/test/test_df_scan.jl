# test_df_scan.jl

@testset "Test standard DataFrame scan" begin
    con = DBInterface.connect(DuckDB.DB)
    df = DataFrame(a = [1, 2, 3], b = [42, 84, 42])

    GC.enable(false)

    DuckDB.RegisterDataFrame(con, df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test names(df) == ["a", "b"]
    @test size(df, 1) == 3
    @test df.a == [1, 2, 3]
    @test df.b == [42, 84, 42]

    DBInterface.close!(con)

    GC.enable(true)
end
