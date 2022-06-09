# test_df_scan.jl

@testset "Test standard DataFrame scan" begin
    con = DBInterface.connect(DuckDB.DB)
    df = DataFrame(a = [1, 2, 3], b = [42, 84, 42])

    DuckDB.register_data_frame(con, df, "my_df")
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

    DuckDB.register_data_frame(con, df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test names(df) == ["a", "b"]
    @test size(df, 1) == 3
    @test isequal(df.a, [1, missing, 3])
    @test isequal(df.b, [missing, 84, missing])

    DBInterface.close!(con)
end

@testset "Test DataFrame scan with numerics" begin
    con = DBInterface.connect(DuckDB.DB)
    numeric_types = [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64]
    for type in numeric_types
        my_df = DataFrame(a = [1, missing, 3], b = [missing, 84, missing])
        my_df[!, :a] = convert.(Union{type, Missing}, my_df[!, :a])
        my_df[!, :b] = convert.(Union{type, Missing}, my_df[!, :b])

        DuckDB.register_data_frame(con, my_df, "my_df")

        results = DBInterface.execute(con, "SELECT * FROM my_df")
        df = DataFrame(results)
        @test isequal(df, my_df)
    end

    DBInterface.close!(con)
end

@testset "Test DataFrame scan with various types" begin
    con = DBInterface.connect(DuckDB.DB)

    # boolean
    my_df = DataFrame(a = [true, false, missing])

    DuckDB.register_data_frame(con, my_df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test isequal(df, my_df)

    # date/time/timestamp
    my_df = DataFrame(
        date = [Date(1992, 9, 20), missing, Date(1950, 2, 3)],
        time = [Time(23, 3, 1), Time(11, 49, 33), missing],
        timestamp = [DateTime(1992, 9, 20, 23, 3, 1), DateTime(1950, 2, 3, 11, 49, 3), missing]
    )

    DuckDB.register_data_frame(con, my_df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test isequal(df, my_df)

    DBInterface.close!(con)
end

@testset "Test DataFrame scan with strings" begin
    con = DBInterface.connect(DuckDB.DB)

    # date/time/timestamp
    my_df = DataFrame(str = ["hello", "this is a very long string", missing, "obligatory mühleisen"])

    DuckDB.register_data_frame(con, my_df, "my_df")

    results = DBInterface.execute(con, "SELECT * FROM my_df")
    df = DataFrame(results)
    @test isequal(df, my_df)

    DBInterface.close!(con)
end

@testset "Test DataFrame scan projection pushdown" begin
    con = DBInterface.connect(DuckDB.DB)
    df = DataFrame(a = [1, 2, 3], b = [42, 84, 42], c = [3, 7, 18])

    DuckDB.register_data_frame(con, df, "my_df")
    GC.gc()

    results = DBInterface.execute(con, "SELECT b FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["b"]
    @test size(df, 1) == 3
    @test df.b == [42, 84, 42]

    results = DBInterface.execute(con, "SELECT c, b FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["c", "b"]
    @test size(df, 1) == 3
    @test df.b == [42, 84, 42]
    @test df.c == [3, 7, 18]

    results = DBInterface.execute(con, "SELECT c, a, a FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["c", "a", "a_1"]
    @test size(df, 1) == 3
    @test df.c == [3, 7, 18]
    @test df.a == [1, 2, 3]
    @test df.a_1 == [1, 2, 3]

    results = DBInterface.execute(con, "SELECT COUNT(*) cnt FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["cnt"]
    @test size(df, 1) == 1
    @test df.cnt == [3]

    GC.gc()

    DBInterface.close!(con)
end

@testset "Test large DataFrame scan" begin
    con = DBInterface.connect(DuckDB.DB)

    my_df = DataFrame(DBInterface.execute(con, "SELECT i%5 AS i FROM range(10000000) tbl(i)"))

    DuckDB.register_data_frame(con, my_df, "my_df")
    GC.gc()

    results = DBInterface.execute(con, "SELECT SUM(i) AS sum FROM my_df")
    GC.gc()
    df = DataFrame(results)
    @test names(df) == ["sum"]
    @test size(df, 1) == 1
    @test df.sum == [20000000]

    DBInterface.close!(con)
end
