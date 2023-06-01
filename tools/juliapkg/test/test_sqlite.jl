# test_sqlite.jl
# tests adopted from SQLite.jl

using Tables

function setup_clean_test_db(f::Function, args...)
    tables = [
        "album",
        "artist",
        "customer",
        "employee",
        "genre",
        "invoice",
        "invoiceline",
        "mediatype",
        "playlist",
        "playlisttrack",
        "track"
    ]
    con = DBInterface.connect(DuckDB.DB)
    datadir = joinpath(@__DIR__, "../data")
    for table in tables
        DBInterface.execute(con, "CREATE TABLE $table AS SELECT * FROM '$datadir/$table.parquet'")
    end

    try
        f(con)
    finally
        close(con)
    end
end

@testset "DB Connection" begin
    con = DBInterface.connect(DuckDB.DB)
    @test con isa DuckDB.DB
    DBInterface.close!(con)
end


@testset "Issue #207: 32 bit integers" begin
    setup_clean_test_db() do db
        ds = DBInterface.execute(db, "SELECT 42::INT64 a FROM Track LIMIT 1") |> columntable
        @test ds.a[1] isa Int64
    end
end

@testset "Regular DuckDB Tests" begin
    setup_clean_test_db() do db
        @test_throws DuckDB.QueryException DBInterface.execute(db, "just some syntax error")
        # syntax correct, table missing
        @test_throws DuckDB.QueryException DBInterface.execute(
            db,
            "SELECT name FROM sqlite_nomaster WHERE type='table';"
        )
    end
end

@testset "close!(query)" begin
    setup_clean_test_db() do db
        qry = DBInterface.execute(db, "SELECT name FROM sqlite_master WHERE type='table';")
        DBInterface.close!(qry)
        return DBInterface.close!(qry) # test it doesn't throw on double-close
    end
end

@testset "Query tables" begin
    setup_clean_test_db() do db
        ds = DBInterface.execute(db, "SELECT name FROM sqlite_master WHERE type='table';") |> columntable
        @test length(ds) == 1
        @test keys(ds) == (:name,)
        @test length(ds.name) == 11
    end
end

@testset "DBInterface.execute([f])" begin
    setup_clean_test_db() do db

        # pipe approach
        results = DBInterface.execute(db, "SELECT * FROM Employee;") |> columntable
        @test length(results) == 15
        @test length(results[1]) == 8
        # callable approach
        @test isequal(DBInterface.execute(columntable, db, "SELECT * FROM Employee"), results)
        employees_stmt = DBInterface.prepare(db, "SELECT * FROM Employee")
        @test isequal(columntable(DBInterface.execute(employees_stmt)), results)
        @test isequal(DBInterface.execute(columntable, employees_stmt), results)
        @testset "throwing from f()" begin
            f(::DuckDB.QueryResult) = error("I'm throwing!")
            @test_throws ErrorException DBInterface.execute(f, employees_stmt)
            @test_throws ErrorException DBInterface.execute(f, db, "SELECT * FROM Employee")
        end
        return DBInterface.close!(employees_stmt)
    end
end

@testset "isempty(::Query)" begin
    setup_clean_test_db() do db

        @test !DBInterface.execute(isempty, db, "SELECT * FROM Employee")
        @test DBInterface.execute(isempty, db, "SELECT * FROM Employee WHERE FirstName='Joanne'")
    end
end

@testset "empty query has correct schema and return type" begin
    setup_clean_test_db() do db
        empty_scheme = DBInterface.execute(Tables.schema, db, "SELECT * FROM Employee WHERE FirstName='Joanne'")
        all_scheme = DBInterface.execute(Tables.schema, db, "SELECT * FROM Employee WHERE FirstName='Joanne'")
        @test empty_scheme.names == all_scheme.names
        @test all(ea -> ea[1] <: ea[2], zip(empty_scheme.types, all_scheme.types))

        empty_tbl = DBInterface.execute(columntable, db, "SELECT * FROM Employee WHERE FirstName='Joanne'")
        all_tbl = DBInterface.execute(columntable, db, "SELECT * FROM Employee")
        @test propertynames(empty_tbl) == propertynames(all_tbl)
    end
end

@testset "Create table, run commit/rollback tests" begin
    setup_clean_test_db() do db
        DBInterface.execute(db, "create table temp as select * from album")
        DBInterface.execute(db, "alter table temp add column colyear int")
        DBInterface.execute(db, "update temp set colyear = 2014")
        r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
        @test length(r) == 4 && length(r[1]) == 10
        @test all(==(2014), r[4])

        @test_throws DuckDB.QueryException DuckDB.rollback(db)
        @test_throws DuckDB.QueryException DuckDB.commit(db)

        DuckDB.transaction(db)
        DBInterface.execute(db, "update temp set colyear = 2015")
        DuckDB.rollback(db)
        r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
        @test all(==(2014), r[4])

        DuckDB.transaction(db)
        DBInterface.execute(db, "update temp set colyear = 2015")
        DuckDB.commit(db)
        r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
        @test all(==(2015), r[4])
    end
end

@testset "Dates" begin
    setup_clean_test_db() do db
        DBInterface.execute(db, "create table temp as select * from album")
        DBInterface.execute(db, "alter table temp add column dates date")
        stmt = DBInterface.prepare(db, "update temp set dates = ?")
        DBInterface.execute(stmt, (Date(2014, 1, 1),))

        r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
        @test length(r) == 4 && length(r[1]) == 10
        @test isa(r[4][1], Date)
        @test all(Bool[x == Date(2014, 1, 1) for x in r[4]])
        return DBInterface.execute(db, "drop table temp")
    end
end

@testset "Prepared Statements" begin
    setup_clean_test_db() do db

        DBInterface.execute(db, "CREATE TABLE temp AS SELECT * FROM Album")
        r = DBInterface.execute(db, "SELECT * FROM temp LIMIT ?", [3]) |> columntable
        @test length(r) == 3 && length(r[1]) == 3
        r = DBInterface.execute(db, "SELECT * FROM temp WHERE Title ILIKE ?", ["%time%"]) |> columntable
        @test r[1] == [76, 111, 187]
        DBInterface.execute(db, "INSERT INTO temp VALUES (?1, ?3, ?2)", [0, 0, "Test Album"])
        r = DBInterface.execute(db, "SELECT * FROM temp WHERE AlbumId = 0") |> columntable
        @test r[1][1] == 0
        @test r[2][1] == "Test Album"
        @test r[3][1] == 0
        DuckDB.drop!(db, "temp")

        DBInterface.execute(db, "CREATE TABLE temp AS SELECT * FROM Album")
        r = DBInterface.execute(db, "SELECT * FROM temp LIMIT ?", (a = 3,)) |> columntable
        @test length(r) == 3 && length(r[1]) == 3
        r = DBInterface.execute(db, "SELECT * FROM temp LIMIT ?", a = 3) |> columntable
        @test length(r) == 3 && length(r[1]) == 3
        r = DBInterface.execute(db, "SELECT * FROM temp WHERE Title ILIKE ?", (word = "%time%",)) |> columntable
        @test r[1] == [76, 111, 187]
        # FIXME: these are supposed to be named parameter tests, but we don't support that yet
        DBInterface.execute(db, "INSERT INTO temp VALUES (?, ?, ?)", (lid = 0, title = "Test Album", rid = 1))
        DBInterface.execute(db, "INSERT INTO temp VALUES (?, ?, ?)", lid = 400, title = "Test2 Album", rid = 3)
        r = DBInterface.execute(db, "SELECT * FROM temp WHERE AlbumId IN (0, 400)") |> columntable
        @test r[1] == [0, 400]
        @test r[2] == ["Test Album", "Test2 Album"]
        @test r[3] == [1, 3]
        return DuckDB.drop!(db, "temp")
    end
end


@testset "DuckDB to Julia type conversion" begin
    binddb = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(
        binddb,
        "CREATE TABLE temp (n INTEGER, i1 INT, i2 integer,
                            f1 REAL, f2 FLOAT, f3 DOUBLE,
                            s1 TEXT, s2 CHAR(10), s3 VARCHAR(15), s4 NVARCHAR(5),
                            d1 DATETIME, ts1 TIMESTAMP)"
    )
    DBInterface.execute(
        binddb,
        "INSERT INTO temp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            missing,
            Int64(6),
            Int64(4),
            6.4,
            6.3,
            Int64(7),
            "some long text",
            "short text",
            "another text",
            "short",
            "2021-02-21",
            "2021-02-12 12:01:32"
        ]
    )
    rr = DBInterface.execute(rowtable, binddb, "SELECT * FROM temp")
    @test length(rr) == 1
    r = first(rr)
    @test typeof.(Tuple(r)) ==
          (Missing, Int32, Int32, Float32, Float32, Float64, String, String, String, String, DateTime, DateTime)
    # Issue #4809: Concrete `String` types.
    # Want to test exactly the types `execute` returns, so check the schema directly and
    # avoid calling `Tuple` or anything else that would narrow the types in the result.
    schema = Tables.schema(rr)
    @test nonmissingtype.(schema.types) ==
          (Int32, Int32, Int32, Float32, Float32, Float64, String, String, String, String, DateTime, DateTime)
end

@testset "Issue #158: Missing DB File" begin
    @test_throws DuckDB.ConnectionException DuckDB.DB("nonexistentdir/not_there.db")
end

@testset "Issue #180, Query" begin
    param = "Hello!"
    query = DBInterface.execute(DuckDB.DB(), "SELECT ?1 UNION ALL SELECT ?1", [param])
    param = "x"
    for row in query
        @test row[1] == "Hello!"
        GC.gc() # this must NOT garbage collect the "Hello!" bound value
    end

    db = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(db, "CREATE TABLE T (a TEXT, PRIMARY KEY (a))")

    q = DBInterface.prepare(db, "INSERT INTO T VALUES(?)")
    DBInterface.execute(q, ["a"])

    @test_throws DuckDB.QueryException DBInterface.execute(q, [1, "a"])
end

@testset "show(DB)" begin
    io = IOBuffer()
    db = DuckDB.DB()

    show(io, db)
    @test String(take!(io)) == "DuckDB.DB(\":memory:\")"

    DBInterface.close!(db)
end

@testset "DuckDB.execute()" begin
    db = DBInterface.connect(DuckDB.DB)
    DBInterface.execute(db, "CREATE TABLE T (x INT UNIQUE)")

    q = DBInterface.prepare(db, "INSERT INTO T VALUES(?)")
    DuckDB.execute(q, (1,))
    r = DBInterface.execute(db, "SELECT * FROM T") |> columntable
    @test r[1] == [1]

    DuckDB.execute(q, [2])
    r = DBInterface.execute(db, "SELECT * FROM T") |> columntable
    @test r[1] == [1, 2]

    q = DBInterface.prepare(db, "INSERT INTO T VALUES(?)")
    DuckDB.execute(q, [3])
    r = DBInterface.execute(columntable, db, "SELECT * FROM T")
    @test r[1] == [1, 2, 3]

    DuckDB.execute(q, [4])
    r = DBInterface.execute(columntable, db, "SELECT * FROM T")
    @test r[1] == [1, 2, 3, 4]

    DuckDB.execute(db, "INSERT INTO T VALUES(?)", [5])
    r = DBInterface.execute(columntable, db, "SELECT * FROM T")
    @test r[1] == [1, 2, 3, 4, 5]

    r = DBInterface.execute(db, strip("   SELECT * FROM T  ")) |> columntable
    @test r[1] == [1, 2, 3, 4, 5]

    r = DBInterface.execute(db, "SELECT * FROM T")
    @test Tables.istable(r)
    @test Tables.rowaccess(r)
    @test Tables.rows(r) === r
    @test Base.IteratorSize(typeof(r)) == Base.SizeUnknown()
    row = first(r)
end

@testset "last_insert_rowid unsupported" begin
    db = DBInterface.connect(DuckDB.DB)
    @test_throws DuckDB.NotImplementedException DBInterface.lastrowid(db)
    @test DuckDB.esc_id(["1", "2", "3"]) == "\"1\",\"2\",\"3\""
end

@testset "Escaping" begin
    @test DuckDB.esc_id(["1", "2", "3"]) == "\"1\",\"2\",\"3\""
end

@testset "Issue #253: Ensure query column names are unique by default" begin
    db = DuckDB.DB()
    res = DBInterface.execute(db, "select 1 as x2, 2 as x2, 3 as x2, 4 as x2_2") |> columntable
    @test res == (x2 = [1], x2_1 = [2], x2_2 = [3], x2_2_1 = [4])
end

@testset "drop!() table name escaping" begin
    db = DuckDB.DB()
    DBInterface.execute(db, "CREATE TABLE \"escape 10.0%\"(i INTEGER)")
    # table exists
    DBInterface.execute(db, "SELECT * FROM \"escape 10.0%\"")
    # drop the table
    DuckDB.drop!(db, "escape 10.0%")
    # it should no longer exist
    @test_throws DuckDB.QueryException DBInterface.execute(db, "SELECT * FROM \"escape 10.0%\"")
end
