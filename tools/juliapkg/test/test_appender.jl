
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

    uuid = Base.UUID("a36a5689-48ec-4104-b147-9fed600d8250")

    # Test data for the appender api test 
    #   - `col_name`: DuckDB column name
    #   - `duck_type`: DuckDB column type 
    #   - `append_value`: Value to insert via DuckDB.append
    #   - `ref_value`: (optional) Expected value from querying the DuckDB table. If not provided, uses `append_value`
    test_data = [
        (; col_name = :bool, duck_type = "BOOLEAN", append_value = true, ref_value = true),
        (; col_name = :tint, duck_type = "TINYINT", append_value = -1, ref_value = Int8(-1)),
        (; col_name = :sint, duck_type = "SMALLINT", append_value = -2, ref_value = Int16(-2)),
        (; col_name = :int, duck_type = "INTEGER", append_value = -3, ref_value = Int32(-3)),
        (; col_name = :bint, duck_type = "BIGINT", append_value = -4, ref_value = Int64(-4)),
        (; col_name = :hint, duck_type = "HUGEINT", append_value = Int128(-5), ref_value = Int128(-5)),
        (; col_name = :utint, duck_type = "UTINYINT", append_value = 1, ref_value = UInt8(1)),
        (; col_name = :usint, duck_type = "USMALLINT", append_value = 2, ref_value = UInt16(2)),
        (; col_name = :uint, duck_type = "UINTEGER", append_value = 3, ref_value = UInt32(3)),
        (; col_name = :ubint, duck_type = "UBIGINT", append_value = 4, ref_value = UInt64(4)),
        (; col_name = :uhint, duck_type = "UHUGEINT", append_value = UInt128(5), ref_value = UInt128(5)),
        (; col_name = :dec16, duck_type = "DECIMAL(4,2)", append_value = FixedDecimal{Int16, 2}(1.01)),
        (; col_name = :dec32, duck_type = "DECIMAL(9,2)", append_value = FixedDecimal{Int32, 2}(1.02)),
        (; col_name = :dec64, duck_type = "DECIMAL(18,2)", append_value = FixedDecimal{Int64, 2}(1.03)),
        (; col_name = :dec128, duck_type = "DECIMAL(38,2)", append_value = FixedDecimal{Int128, 2}(1.04)),
        (; col_name = :float, duck_type = "FLOAT", append_value = 1.0, ref_value = Float32(1.0)),
        (; col_name = :double, duck_type = "DOUBLE", append_value = 2.0, ref_value = Float64(2.0)),
        (; col_name = :date, duck_type = "DATE", append_value = Dates.Date("1970-04-11")),
        (; col_name = :time, duck_type = "TIME", append_value = Dates.Time(0, 0, 0, 0, 200)),
        (; col_name = :timestamp, duck_type = "TIMESTAMP", append_value = Dates.DateTime("1970-01-02T01:23:45.678")),
        (; col_name = :missingval, duck_type = "INTEGER", append_value = missing),
        (; col_name = :nothingval, duck_type = "INTEGER", append_value = nothing, ref_value = missing),
        (; col_name = :largeval, duck_type = "INTEGER", append_value = Int32(2^16)),
        (; col_name = :uuid, duck_type = "UUID", append_value = uuid),
        (; col_name = :varchar, duck_type = "VARCHAR", append_value = "Foo")
    ]

    sql = """CREATE TABLE dtypes(
               $(join(("$(row.col_name) $(row.duck_type)" for row in test_data), ",\n"))
            )"""
    DuckDB.execute(db, sql)
    appender = DuckDB.Appender(db, "dtypes")
    for row in test_data
        DuckDB.append(appender, row.append_value)
    end
    # End the row of the appender
    DuckDB.end_row(appender)
    # Destroy the appender and flush the data
    DuckDB.flush(appender)
    DuckDB.close(appender)

    results = DBInterface.execute(db, "select * from dtypes;")
    df = DataFrame(results)
    for row in test_data
        ref_value = get(row, :ref_value, row.append_value)
        @test isequal(df[!, row.col_name], [ref_value])
    end

    # close the database 
    DBInterface.close!(db)
end
