
@testset "Appender Error" begin
    db = DBInterface.connect(DuckDB.DB)
    con = DBInterface.connect(db)

    @test_throws DuckDB.QueryException DuckDB.Appender(db, "nonexistanttable")
    @test_throws DuckDB.QueryException DuckDB.Appender(con, "t")
end

@testset "Appender Usage" begin
    db = DBInterface.connect(DuckDB.DB)

    DBInterface.execute(db, "CREATE TABLE integers(i INTEGER)")

    appender = DuckDB.Appender(db, "integers")
    DuckDB.close(appender)
    DuckDB.close(appender)

    appender = DuckDB.Appender(db, "integers")
    for i in 0:9
        DuckDB.append(appender, i)
        DuckDB.end_row(appender)
    end
    DuckDB.flush(appender)

    results = DBInterface.execute(db, "SELECT * FROM integers")
    df = DataFrame(results)
    @test names(df) == ["i"]
    @test size(df, 1) == 10
    @test df.i == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
end

# @testset "Appender API" begin
#     # Open the database
#     db = DuckDB.open(":memory:")
#     con = DuckDB.connect(db)
#
#     # Create the table the data is appended to
#     DuckDB.execute(con, "CREATE TABLE dtypes(bol BOOLEAN, tint TINYINT, sint SMALLINT, int INTEGER, bint BIGINT, utint UTINYINT, usint USMALLINT, uint UINTEGER, ubint UBIGINT, float FLOAT, double DOUBLE, date DATE, time TIME, vchar VARCHAR, nullval INTEGER)")
#
#     # Create the appender
#     appender = DuckDB.appender_create(con, "dtypes")
#
#     # Append the different data types
#     DuckDB.duckdb_append_bool(appender, true)
#     DuckDB.duckdb_append_int8(appender, 1)
#     DuckDB.duckdb_append_int16(appender, 2)
#     DuckDB.duckdb_append_int32(appender, 3)
#     DuckDB.duckdb_append_int64(appender, 4)
#     DuckDB.duckdb_append_uint8(appender, 1)
#     DuckDB.duckdb_append_uint16(appender, 2)
#     DuckDB.duckdb_append_uint32(appender, 3)
#     DuckDB.duckdb_append_uint64(appender, 4)
#     DuckDB.duckdb_append_float(appender, 1.0)
#     DuckDB.duckdb_append_double(appender, 2.0)
#     DuckDB.duckdb_append_date(appender, 100)
#     DuckDB.duckdb_append_time(appender, 200)
#     DuckDB.duckdb_append_varchar(appender, "Foo")
#     DuckDB.duckdb_append_null(appender)
#     # End the row of the appender
#     DuckDB.duckdb_appender_end_row(appender)
#     # Destroy the appender and flush the data
#     DuckDB.duckdb_appender_destroy(appender)
#
#     # Retrive the data from the table and store it in  a vector
#     df = DuckDB.toDataFrame(con, "select * from dtypes;")
#     data = Matrix(df)
#
#     # Test if the correct types have been appended to the table
#     @test data[1] === true
#     @test data[2] === Int8(1)
#     @test data[3] === Int16(2)
#     @test data[4] === Int32(3)
#     @test data[5] === Int64(4)
#     @test data[6] === UInt8(1)
#     @test data[7] === UInt16(2)
#     @test data[8] === UInt32(3)
#     @test data[9] === UInt64(4)
#     @test data[10] === Float32(1.0)
#     @test data[11] === Float64(2.0)
#     @test data[12] === Dates.Date("1970-04-11")
#     @test data[13] === Dates.Time(0, 0, 0, 0, 200)
#     @test data[14] === "Foo"
#     @test data[15] === missing
#
#     # Disconnect and close the database
#     DuckDB.disconnect(con)
#     DuckDB.close(db)
# end
