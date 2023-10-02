# test_old_interface.jl

@testset "DB Connection" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    @test isa(con, DuckDB.Connection)
    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "Test append DataFrame" begin
    # Open the database
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)

    # Create the table the data is appended to
    DuckDB.execute(
        con,
        "CREATE TABLE dtypes(bool BOOLEAN, tint TINYINT, sint SMALLINT, int INTEGER, bint BIGINT, utint UTINYINT, usint USMALLINT, uint UINTEGER, ubint UBIGINT, float FLOAT, double DOUBLE, date DATE, time TIME, vchar VARCHAR, nullval INTEGER)"
    )

    # Create test DataFrame
    input_df = DataFrame(
        bool = [true, false],
        tint = Int8.(1:2),
        sint = Int16.(1:2),
        int = Int32.(1:2),
        bint = Int64.(1:2),
        utint = UInt8.(1:2),
        usint = UInt16.(1:2),
        uint = UInt32.(1:2),
        ubint = UInt64.(1:2),
        float = Float32.(1:2),
        double = Float64.(1:2),
        date = [Dates.Date("1970-04-11"), Dates.Date("1970-04-12")],
        time = [Dates.Time(0, 0, 0, 100, 0), Dates.Time(0, 0, 0, 200, 0)],
        vchar = ["Foo", "Bar"],
        nullval = [missing, Int32(2)]
    )

    # append the DataFrame to the table
    DuckDB.appendDataFrame(input_df, con, "dtypes")

    # Output the data from the table
    output_df = DataFrame(DuckDB.toDataFrame(con, "select * from dtypes;"))

    # Compare each column of the input and output dataframe with each other
    for (col_pos, input_col) in enumerate(eachcol(input_df))
        @test isequal(input_col, output_df[:, col_pos])
    end

    # Disconnect and close the database
    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "Test README" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    res = DuckDB.execute(con, "CREATE TABLE integers(date DATE, jcol INTEGER)")
    res = DuckDB.execute(con, "INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8)")
    res = DuckDB.execute(con, "SELECT * FROM integers")
    df = DataFrame(DuckDB.toDataFrame(res))
    @test isa(df, DataFrame)
    df = DataFrame(DuckDB.toDataFrame(con, "SELECT * FROM integers"))
    println(typeof(df))
    @test isa(df, DataFrame)
    DuckDB.appendDataFrame(df, con, "integers")
    DuckDB.disconnect(con)
    DuckDB.close(db)
end
#
@testset "HUGE Int test" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    res = DuckDB.execute(con, "CREATE TABLE huge(id INTEGER,data HUGEINT);")
    res = DuckDB.execute(con, "INSERT INTO huge VALUES (1,NULL), (2, 1761718171), (3, 171661889178);")
    res = DuckDB.toDataFrame(con, "SELECT * FROM huge")
    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "Interval type" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    res = DuckDB.execute(con, "CREATE TABLE interval(interval INTERVAL);")
    res = DuckDB.execute(
        con,
        """
INSERT INTO interval VALUES
(INTERVAL 5 HOUR),
(INTERVAL 12 MONTH),
(INTERVAL 12 MICROSECOND),
(INTERVAL 1 YEAR);
"""
    )
    res = DataFrame(DuckDB.toDataFrame(con, "SELECT * FROM interval;"))
    @test isa(res, DataFrame)
    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "Timestamp" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)

    # insert without timezone, display as UTC
    res = DuckDB.execute(con, "CREATE TABLE timestamp(timestamp TIMESTAMP , data INTEGER);")
    res = DuckDB.execute(
        con,
        "INSERT INTO timestamp VALUES ('2021-09-27 11:30:00.000', 4), ('2021-09-28 12:30:00.000', 6), ('2021-09-29 13:30:00.000', 8);"
    )
    res = DuckDB.execute(con, "SELECT * FROM timestamp WHERE timestamp='2021-09-27T11:30:00Z';")
    df = DataFrame(res)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 11, 30, 0))

    # insert with timezone, display as UTC
    res = DuckDB.execute(con, "CREATE TABLE timestamp1(timestamp TIMESTAMP , data INTEGER);")
    res = DuckDB.execute(
        con,
        "INSERT INTO timestamp1 VALUES ('2021-09-27T11:30:00.000+01', 4), ('2021-09-28T12:30:00.000+01', 6), ('2021-09-29T13:30:00.000+01', 8);"
    )
    res = DuckDB.execute(con, "SELECT * FROM timestamp1 WHERE timestamp=?;", [DateTime(2021, 9, 27, 10, 30, 0)])
    df = DataFrame(res)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 10, 30, 0))

    # query with local datetime, display as UTC
    res = DuckDB.execute(con, "SELECT * FROM timestamp1 WHERE timestamp='2021-09-27T11:30:00.000+01';")
    df = DataFrame(res)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 10, 30, 0))

    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "TimestampTZ" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    DuckDB.execute(con, "SET TimeZone='Asia/Shanghai'") # UTC+8

    res = DuckDB.execute(con, "SELECT TIMESTAMPTZ '2021-09-27 11:30:00' tz, TIMESTAMP '2021-09-27 11:30:00' ts;")
    df = DataFrame(res)
    @test isequal(df[1, "tz"], DateTime(2021, 9, 27, 3, 30, 0))
    @test isequal(df[1, "ts"], DateTime(2021, 9, 27, 11, 30, 0))

    res = DuckDB.execute(con, "CREATE TABLE timestamptz(timestamp TIMESTAMPTZ , data INTEGER);")
    res = DuckDB.execute(
        con,
        "INSERT INTO timestamptz VALUES ('2021-09-27 11:30:00.000', 4), ('2021-09-28 12:30:00.000', 6), ('2021-09-29 13:30:00.000', 8);"
    )
    res = DuckDB.execute(con, "SELECT * FROM timestamptz WHERE timestamp='2021-09-27 11:30:00'")
    df = DataFrame(res)
    @test isequal(df[1, "data"], 4)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 3, 30, 0))

    res = DuckDB.execute(con, "SELECT * FROM timestamptz WHERE timestamp='2021-09-27T03:30:00Z'")
    df = DataFrame(res)
    @test isequal(df[1, "data"], 4)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 3, 30, 0))

    res = DuckDB.execute(con, "SELECT * FROM timestamptz WHERE timestamp='2021-09-27T12:30:00+09'")
    df = DataFrame(res)
    @test isequal(df[1, "data"], 4)
    @test isequal(df[1, "timestamp"], DateTime(2021, 9, 27, 3, 30, 0))

    DuckDB.disconnect(con)
    DuckDB.close(db)
end

@testset "Items table" begin
    db = DuckDB.open(":memory:")
    con = DuckDB.connect(db)
    res = DuckDB.execute(con, "CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER);")
    res = DuckDB.execute(con, "INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2);")
    res = DataFrame(DuckDB.toDataFrame(con, "SELECT * FROM items;"))
    @test isa(res, DataFrame)
    DuckDB.disconnect(con)
end

@testset "Integers and dates table" begin
    db = DuckDB.DB()
    res = DBInterface.execute(db, "CREATE TABLE integers(date DATE, data INTEGER);")
    res =
        DBInterface.execute(db, "INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8);")
    res = DBInterface.execute(db, "SELECT * FROM integers;")
    res = DataFrame(DuckDB.toDataFrame(res))
    @test res.date == [Date(2021, 9, 27), Date(2021, 9, 28), Date(2021, 9, 29)]
    @test isa(res, DataFrame)
    DBInterface.close!(db)
end
