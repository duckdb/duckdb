# test_decimals.jl


@testset "Test decimal support" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(
        con,
        "SELECT 42.3::DECIMAL(4,1) a, 4923.3::DECIMAL(9,1) b, 421.423::DECIMAL(18,3) c, 129481294.3392::DECIMAL(38,4) d"
    )

    # convert to DataFrame
    df = DataFrame(results)
    @test names(df) == ["a", "b", "c", "d"]
    @test size(df, 1) == 1
    @test df.a == [42.3]
    @test df.b == [4923.3]
    @test df.c == [421.423]
    @test df.d == [129481294.3392]

    DBInterface.close!(con)
end
