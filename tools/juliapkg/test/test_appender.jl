
@testset "Appender Error" begin
    db = DBInterface.connect(DuckDB.DB)
    con = DBInterface.connect(db)

    @test_throws DuckDB.QueryException DuckDB.Appender(db, "nonexistanttable")
    @test_throws DuckDB.QueryException DuckDB.Appender(con, "t")
end

@testset "Appender Usage - Schema $(schema_provided ? "Provided" : "Not Provided")" for schema_provided in (false, true)
    db = DBInterface.connect(DuckDB.DB)

    table_name = "integers"
    if schema_provided
        schema_name = "test"
        full_table_name = "$(schema_name).$(table_name)"
        DBInterface.execute(db, "CREATE SCHEMA $(schema_name)")
    else
        schema_name = nothing
        full_table_name = table_name
    end

    DBInterface.execute(db, "CREATE TABLE $(full_table_name)(i INTEGER)")

    appender = DuckDB.Appender(db, table_name, schema_name)
    DuckDB.close(appender)
    DuckDB.close(appender)

    # close!
    appender = DuckDB.Appender(db, table_name, schema_name)
    DBInterface.close!(appender)

    appender = DuckDB.Appender(db, table_name, schema_name)
    for i in 0:9
        DuckDB.append(appender, i)
        DuckDB.end_row(appender)
    end
    DuckDB.flush(appender)
    DuckDB.close(appender)

    results = DBInterface.execute(db, "SELECT * FROM $(full_table_name)")
    df = DataFrame(results)
    @test names(df) == ["i"]
    @test size(df, 1) == 10
    @test df.i == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    # close the database 
    DuckDB.close(appender)
end

@testset "Appender API" begin
    # Open the database
    db = DBInterface.connect(DuckDB.DB)

    # Create the table the data is appended to
    DuckDB.execute(
        db,
        "CREATE TABLE dtypes(
            bool BOOLEAN, 
            tint TINYINT, 
            sint SMALLINT, 
            int INTEGER, 
            bint BIGINT, 
            hint HUGEINT,
            utint UTINYINT, 
            usint USMALLINT, 
            uint UINTEGER, 
            ubint UBIGINT, 
            uhint UHUGEINT,
            float FLOAT, 
            double DOUBLE, 
            date DATE, 
            time TIME,
            timestamp TIMESTAMP, 
            missingval INTEGER,
            nothingval INTEGER,
            uuid UUID,
            varchar VARCHAR)"
    )

    # Create the appender
    appender = DuckDB.Appender(db, "dtypes")

    # Append the different data types
    DuckDB.append(appender, true)
    DuckDB.append(appender, -1)
    DuckDB.append(appender, -2)
    DuckDB.append(appender, -3)
    DuckDB.append(appender, -4)
    DuckDB.append(appender, Int128(-5))
    DuckDB.append(appender, 1)
    DuckDB.append(appender, 2)
    DuckDB.append(appender, 3)
    DuckDB.append(appender, 4)
    DuckDB.append(appender, UInt128(5))
    DuckDB.append(appender, 1.0)
    DuckDB.append(appender, 2.0)
    DuckDB.append(appender, Dates.Date("1970-04-11"))
    DuckDB.append(appender, Dates.Time(0, 0, 0, 0, 200))
    DuckDB.append(appender, Dates.DateTime("1970-01-02T01:23:45.678"))
    DuckDB.append(appender, missing)
    DuckDB.append(appender, nothing)
    uuid = Base.UUID("a36a5689-48ec-4104-b147-9fed600d8250")
    DuckDB.append(appender, uuid)
    DuckDB.append(appender, "Foo")
    # End the row of the appender
    DuckDB.end_row(appender)
    # Destroy the appender and flush the data
    DuckDB.flush(appender)
    DuckDB.close(appender)

    # Retrive the data from the table and store it in  a vector
    results = DBInterface.execute(db, "select * from dtypes;")
    df = DataFrame(results)

    # Test if the correct types have been appended to the table
    @test df.bool == [true]
    @test df.tint == [Int8(-1)]
    @test df.sint == [Int16(-2)]
    @test df.int == [Int32(-3)]
    @test df.bint == [Int64(-4)]
    @test df.hint == [Int128(-5)]
    @test df.utint == [UInt8(1)]
    @test df.usint == [UInt16(2)]
    @test df.uint == [UInt32(3)]
    @test df.ubint == [UInt64(4)]
    @test df.uhint == [UInt128(5)]
    @test df.float == [Float32(1.0)]
    @test df.double == [Float64(2.0)]
    @test df.date == [Dates.Date("1970-04-11")]
    @test df.time == [Dates.Time(0, 0, 0, 0, 200)]
    @test df.timestamp == [Dates.DateTime("1970-01-02T01:23:45.678")]
    @test isequal(df.missingval, [missing])
    @test isequal(df.nothingval, [missing])
    @test df.uuid == [uuid]
    @test df.varchar == ["Foo"]

    # close the database 
    DBInterface.close!(db)
end
