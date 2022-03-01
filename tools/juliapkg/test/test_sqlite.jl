# test_sqlite.jl
# tests adopted from SQLite.jl

using SQLite
using Test, Dates, Random, WeakRefStrings, Tables, DBInterface

import Base: +, ==

mutable struct Point{T}
    x::T
    y::T
end

==(a::Point, b::Point) = a.x == b.x && a.y == b.y

mutable struct Point3D{T<:Number}
    x::T
    y::T
    z::T
end

==(a::Point3D, b::Point3D) = a.x == b.x && a.y == b.y && a.z == b.z
+(a::Point3D, b::Point3D) = Point3D(a.x + b.x, a.y + b.y, a.z + b.z)

triple(x) = 3x

function add4(q)
    q + 4
end

mult(args...) = *(args...)
str2arr(s) = Vector{UInt8}(s)
doublesum_step(persist, current) = persist + current
doublesum_final(persist) = 2 * persist
mycount(p, c) = p + 1
bigsum(p, c) = p + big(c)
sumpoint(p::Point3D, x, y, z) = p + Point3D(x, y, z)


"""
    setup_clean_test_db() do db
        @test db isa SQLite.DB
    end
Copy test sqlite file to temp directory, path `test.sqlite`,
overwrite file if necessary and set permissions.
"""
function setup_clean_test_db(f::Function, args...)
    dbfile = joinpath(dirname(pathof(SQLite)), "../test/Chinook_Sqlite.sqlite")
    tmp_dir = mktempdir()
    test_dbfile = joinpath(tmp_dir, "test.sqlite")

    cp(dbfile, test_dbfile; force = true)
    chmod(test_dbfile, 0o777)

    db = SQLite.DB(test_dbfile)
    try
        f(db)
    finally
        close(db)
        rm(tmp_dir, recursive = true)
    end
end

@testset "basics" begin

    @testset "Julia to SQLite3 type conversion" begin
        @test SQLite.sqlitetype(Int) == "INT NOT NULL"
        @test SQLite.sqlitetype(Union{Float64,Missing}) == "REAL"
        @test SQLite.sqlitetype(String) == "TEXT NOT NULL"
        @test SQLite.sqlitetype(Symbol) == "BLOB NOT NULL"
        @test SQLite.sqlitetype(Missing) == "NULL"
        @test SQLite.sqlitetype(Nothing) == "NULL"
        @test SQLite.sqlitetype(Any) == "BLOB"
    end

    @testset "DB Connection" begin
        dbfile = joinpath(dirname(pathof(SQLite)), "../test/Chinook_Sqlite.sqlite")
        @test SQLite.DB(dbfile) isa SQLite.DB
        con = DBInterface.connect(SQLite.DB, dbfile)
        @test con isa SQLite.DB
        DBInterface.close!(con)
    end

    @testset "SQLite.tables(db)" begin
        setup_clean_test_db() do db

            results1 = SQLite.tables(db)

            @test isa(results1, SQLite.DBTables)
            @test length(results1) == 11
            @test isa(results1[1], SQLite.DBTable)

            @test Tables.istable(results1)
            @test Tables.rowaccess(results1)
            @test Tables.rows(results1) == results1

            @test results1[1].name == "Album"
            @test results1[1].schema == Tables.schema(DBInterface.execute(db,"SELECT * FROM Album LIMIT 0"))

            @test SQLite.DBTable("Album") == SQLite.DBTable("Album", nothing)

            @test [t.name for t in results1] == ["Album", "Artist", "Customer", "Employee", "Genre", "Invoice", "InvoiceLine", "MediaType", "Playlist", "PlaylistTrack", "Track"]
        end

        @testset "#209: SQLite.tables response when no tables in DB" begin
            db = SQLite.DB()
            tables_v = SQLite.tables(db)
            @test String[] == [t.name for t in tables_v]
            DBInterface.close!(db)
        end
    end

    @testset "Issue #207: 32 bit integers" begin
        setup_clean_test_db() do db
            ds =
                DBInterface.execute(db, "SELECT RANDOM() as a FROM Track LIMIT 1") |>
                columntable
            @test ds.a[1] isa Int64

        end
    end

    @testset "Regular SQLite Tests" begin
        setup_clean_test_db() do db
            @test_throws SQLiteException DBInterface.execute(db, "just some syntax error")
            # syntax correct, table missing
            @test_throws SQLiteException DBInterface.execute(
                db,
                "SELECT name FROM sqlite_nomaster WHERE type='table';",
            )
        end
    end

    @testset "close!(query)" begin
        setup_clean_test_db() do db
            qry = DBInterface.execute(db, "SELECT name FROM sqlite_master WHERE type='table';")
            DBInterface.close!(qry)
            DBInterface.close!(qry) # test it doesn't throw on double-close
        end
    end

    @testset "Query tables" begin
        setup_clean_test_db() do db
            ds =
                DBInterface.execute(db, "SELECT name FROM sqlite_master WHERE type='table';") |>
                columntable
            @test length(ds) == 1
            @test keys(ds) == (:name,)
            @test length(ds.name) == 11

            results1 = SQLite.tables(db)
            @test isequal(ds.name, [t.name for t in results1])
        end
    end

    @testset "DBInterface.execute([f])" begin
        setup_clean_test_db() do db

            # pipe approach
            results = DBInterface.execute(db, "SELECT * FROM Employee;") |> columntable
            @test length(results) == 15
            @test length(results[1]) == 8
            # callable approach
            @test isequal(
                DBInterface.execute(columntable, db, "SELECT * FROM Employee"),
                results,
            )
            employees_stmt = DBInterface.prepare(db, "SELECT * FROM Employee")
            @test isequal(columntable(DBInterface.execute(employees_stmt)), results)
            @test isequal(DBInterface.execute(columntable, employees_stmt), results)
            @testset "throwing from f()" begin
                f(::SQLite.Query) = error("I'm throwing!")
                @test_throws ErrorException DBInterface.execute(f, employees_stmt)
                @test_throws ErrorException DBInterface.execute(f, db, "SELECT * FROM Employee")
            end
            DBInterface.close!(employees_stmt)
        end
    end

    @testset "isempty(::Query)" begin
        setup_clean_test_db() do db

            @test !DBInterface.execute(isempty, db, "SELECT * FROM Employee")
            @test DBInterface.execute(
                isempty,
                db,
                "SELECT * FROM Employee WHERE FirstName='Joanne'",
            )
        end
    end

    @testset "empty query has correct schema and return type" begin
        setup_clean_test_db() do db
            empty_scheme = DBInterface.execute(
                Tables.schema,
                db,
                "SELECT * FROM Employee WHERE FirstName='Joanne'",
            )
            all_scheme = DBInterface.execute(
                Tables.schema,
                db,
                "SELECT * FROM Employee WHERE FirstName='Joanne'",
            )
            @test empty_scheme.names == all_scheme.names
            @test all(ea -> ea[1] <: ea[2], zip(empty_scheme.types, all_scheme.types))

            empty_tbl = DBInterface.execute(
                columntable,
                db,
                "SELECT * FROM Employee WHERE FirstName='Joanne'",
            )
            all_tbl = DBInterface.execute(columntable, db, "SELECT * FROM Employee")
            @test propertynames(empty_tbl) == propertynames(all_tbl)
            @test all(
                col ->
                    eltype(empty_tbl[col]) == Missing ||
                        eltype(empty_tbl[col]) >: eltype(all_tbl[col]),
                propertynames(all_tbl),
            )
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

            @test_throws SQLiteException SQLite.rollback(db)
            @test_throws SQLiteException SQLite.commit(db)

            SQLite.transaction(db)
            DBInterface.execute(db, "update temp set colyear = 2015")
            SQLite.rollback(db)
            r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
            @test all(==(2014), r[4])

            SQLite.transaction(db)
            DBInterface.execute(db, "update temp set colyear = 2015")
            SQLite.commit(db)
            r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
            @test all(==(2015), r[4])
        end
    end

    @testset "Dates" begin
        setup_clean_test_db() do db
            DBInterface.execute(db, "create table temp as select * from album")
            DBInterface.execute(db, "alter table temp add column dates blob")
            stmt = DBInterface.prepare(db, "update temp set dates = ?")
            DBInterface.execute(stmt, (Date(2014, 1, 1),))

            r = DBInterface.execute(db, "select * from temp limit 10") |> columntable
            @test length(r) == 4 && length(r[1]) == 10
            @test isa(r[4][1], Date)
            @test all(Bool[x == Date(2014, 1, 1) for x in r[4]])
            DBInterface.execute(db, "drop table temp")

            rng = Dates.Date(2013):Dates.Day(1):Dates.Date(2013, 1, 5)
            dt = (i = collect(rng), j = collect(rng))
            tablename = dt |> SQLite.load!(db, "temp")
            r = DBInterface.execute(db, "select * from $tablename") |> columntable
            @test length(r) == 2 && length(r[1]) == 5
            @test all([i for i in r[1]] .== collect(rng))
            @test all([isa(i, Dates.Date) for i in r[1]])
            SQLite.drop!(db, "$tablename")
        end
    end

    @testset "Prepared Statements" begin
        setup_clean_test_db() do db

            DBInterface.execute(db, "CREATE TABLE temp AS SELECT * FROM Album")
            r = DBInterface.execute(db, "SELECT * FROM temp LIMIT ?", [3]) |> columntable
            @test length(r) == 3 && length(r[1]) == 3
            r =
                DBInterface.execute(db, "SELECT * FROM temp WHERE Title LIKE ?", ["%time%"]) |>
                columntable
            @test r[1] == [76, 111, 187]
            DBInterface.execute(
                db,
                "INSERT INTO temp VALUES (?1, ?3, ?2)",
                [0, 0, "Test Album"],
            )
            r = DBInterface.execute(db, "SELECT * FROM temp WHERE AlbumId = 0") |> columntable
            @test r[1][1] == 0
            @test r[2][1] == "Test Album"
            @test r[3][1] == 0
            SQLite.drop!(db, "temp")

            DBInterface.execute(db, "CREATE TABLE temp AS SELECT * FROM Album")
            r = DBInterface.execute(db, "SELECT * FROM temp LIMIT :a", (a = 3,)) |> columntable
            @test length(r) == 3 && length(r[1]) == 3
            r = DBInterface.execute(db, "SELECT * FROM temp LIMIT :a", a = 3) |> columntable
            @test length(r) == 3 && length(r[1]) == 3
            r =
                DBInterface.execute(
                    db,
                    "SELECT * FROM temp WHERE Title LIKE @word",
                    (word = "%time%",),
                ) |> columntable
            @test r[1] == [76, 111, 187]
            DBInterface.execute(
                db,
                "INSERT INTO temp VALUES (@lid, :title, \$rid)",
                (rid = 1, lid = 0, title = "Test Album"),
            )
            DBInterface.execute(
                db,
                "INSERT INTO temp VALUES (@lid, :title, \$rid)",
                rid = 3,
                lid = 400,
                title = "Test2 Album",
            )
            r =
                DBInterface.execute(db, "SELECT * FROM temp WHERE AlbumId IN (0, 400)") |>
                columntable
            @test r[1] == [0, 400]
            @test r[2] == ["Test Album", "Test2 Album"]
            @test r[3] == [1, 3]
            SQLite.drop!(db, "temp")

            SQLite.register(db, SQLite.regexp, nargs = 2, name = "regexp")
            r =
                DBInterface.execute(
                    db,
                    SQLite.@sr_str(
                        "SELECT LastName FROM Employee WHERE BirthDate REGEXP '^\\d{4}-08'"
                    )
                ) |> columntable
            @test r[1][1] == "Peacock"

            SQLite.register(db, identity, nargs = 1, name = "identity")
            r =
                DBInterface.execute(
                    db,
                    """SELECT identity("abc") as x, "abc" == identity("abc") as cmp""",
                ) |> columntable
            @test first(r.x) == "abc"
            @test first(r.cmp) == 1

            @test_throws AssertionError SQLite.register(db, triple, nargs = 186)
            SQLite.register(db, triple, nargs = 1)
            r =
                DBInterface.execute(
                    db,
                    "SELECT triple(Total) FROM Invoice ORDER BY InvoiceId LIMIT 5",
                ) |> columntable
            s =
                DBInterface.execute(
                    db,
                    "SELECT Total FROM Invoice ORDER BY InvoiceId LIMIT 5",
                ) |> columntable
            for (i, j) in zip(r[1], s[1])
                @test abs(i - 3 * j) < 0.02
            end
        end
    end

    @testset "Register functions" begin
        setup_clean_test_db() do db
            SQLite.@register db add4
            r = DBInterface.execute(db, "SELECT add4(AlbumId) FROM Album") |> columntable
            s = DBInterface.execute(db, "SELECT AlbumId FROM Album") |> columntable
            @test r[1][1] == s[1][1] + 4

            SQLite.@register db mult
            r = DBInterface.execute(db, "SELECT GenreId, UnitPrice FROM Track") |> columntable
            s =
                DBInterface.execute(db, "SELECT mult(GenreId, UnitPrice) FROM Track") |>
                columntable
            @test (r[1][1] * r[2][1]) == s[1][1]
            t =
                DBInterface.execute(db, "SELECT mult(GenreId, UnitPrice, 3, 4) FROM Track") |>
                columntable
            @test (r[1][1] * r[2][1] * 3 * 4) == t[1][1]

            SQLite.@register db sin
            u =
                DBInterface.execute(db, "select sin(milliseconds) from track limit 5") |>
                columntable
            @test all(-1 .< convert(Vector{Float64}, u[1]) .< 1)

            SQLite.register(db, hypot; nargs = 2, name = "hypotenuse")
            v =
                DBInterface.execute(
                    db,
                    "select hypotenuse(Milliseconds,bytes) from track limit 5",
                ) |> columntable
            @test [round(Int, i) for i in v[1]] == [11175621, 5521062, 3997652, 4339106, 6301714]

            SQLite.@register db str2arr
            r =
                DBInterface.execute(db, "SELECT str2arr(LastName) FROM Employee LIMIT 2") |>
                columntable
            @test r[1][2] == UInt8[0x45, 0x64, 0x77, 0x61, 0x72, 0x64, 0x73]

            SQLite.@register db big
            r = DBInterface.execute(db, "SELECT big(5)") |> columntable
            @test r[1][1] == big(5)
            @test typeof(r[1][1]) == BigInt

            SQLite.register(db, 0, doublesum_step, doublesum_final, name = "doublesum")
            r = DBInterface.execute(db, "SELECT doublesum(UnitPrice) FROM Track") |> columntable
            s = DBInterface.execute(db, "SELECT UnitPrice FROM Track") |> columntable
            @test abs(r[1][1] - 2 * sum(convert(Vector{Float64}, s[1]))) < 0.02


            SQLite.register(db, 0, mycount)
            r =
                DBInterface.execute(db, "SELECT mycount(TrackId) FROM PlaylistTrack") |>
                columntable
            s =
                DBInterface.execute(db, "SELECT count(TrackId) FROM PlaylistTrack") |>
                columntable
            @test r[1][1] == s[1][1]

            SQLite.register(db, big(0), bigsum)
            r =
                DBInterface.execute(db, "SELECT bigsum(TrackId) FROM PlaylistTrack") |>
                columntable
            s = DBInterface.execute(db, "SELECT TrackId FROM PlaylistTrack") |> columntable
            @test r[1][1] == big(sum(convert(Vector{Int}, s[1])))

            DBInterface.execute(db, "CREATE TABLE points (x INT, y INT, z INT)")
            DBInterface.execute(db, "INSERT INTO points VALUES (?, ?, ?)", (1, 2, 3))
            DBInterface.execute(db, "INSERT INTO points VALUES (?, ?, ?)", (4, 5, 6))
            DBInterface.execute(db, "INSERT INTO points VALUES (?, ?, ?)", (7, 8, 9))

            SQLite.register(db, Point3D(0, 0, 0), sumpoint)
            r = DBInterface.execute(db, "SELECT sumpoint(x, y, z) FROM points") |> columntable
            @test r[1][1] == Point3D(12, 15, 18)
            SQLite.drop!(db, "points")

            db2 = DBInterface.connect(SQLite.DB)
            DBInterface.execute(db2, "CREATE TABLE tab1 (r REAL, s INT)")

            @test_throws SQLiteException SQLite.drop!(db2, "nonexistant")
            # should not throw anything
            SQLite.drop!(db2, "nonexistant", ifexists = true)
            # should drop "tab2"
            SQLite.drop!(db2, "tab2", ifexists = true)
            @test filter(x -> x.name == "tab2", SQLite.tables(db2)) |> length == 0

            SQLite.drop!(db, "sqlite_stat1", ifexists = true)
            tables = SQLite.tables(db)
            @test length(tables) == 11
        end
    end

    @testset "Remove Duplicates" begin
        setup_clean_test_db() do db
            db = SQLite.DB() #In case the order of tests is changed
            dt = (ints = Int64[1, 1, 2, 2, 3], strs = ["A", "A", "B", "C", "C"])
            tablename = dt |> SQLite.load!(db, "temp")
            SQLite.removeduplicates!(db, "temp", ["ints", "strs"]) #New format
            dt3 = DBInterface.execute(db, "Select * from temp") |> columntable
            @test dt3[1][1] == 1
            @test dt3[2][1] == "A"
            @test dt3[1][2] == 2
            @test dt3[2][2] == "B"
            @test dt3[1][3] == 2
            @test dt3[2][3] == "C"
        end
    end


    @testset "Issue #104: bind!() fails with named parameters" begin
        db = SQLite.DB() #In case the order of tests is changed
        DBInterface.execute(db, "CREATE TABLE IF NOT EXISTS tbl(a  INTEGER);")
        stmt = DBInterface.prepare(db, "INSERT INTO tbl (a) VALUES (@a);")
        SQLite.bind!(stmt, "@a", 1)
        SQLite.clear!(stmt)
    end

    @testset "SQLite to Julia type conversion" begin
        binddb = SQLite.DB()
        DBInterface.execute(
            binddb,
            "CREATE TABLE temp (n NULL, i1 INT, i2 integer,
                                f1 REAL, f2 FLOAT, f3 NUMERIC,
                                s1 TEXT, s2 CHAR(10), s3 VARCHAR(15), s4 NVARCHAR(5),
                                d1 DATETIME, ts1 TIMESTAMP,
                                b BLOB,
                                x1 UNKNOWN1, x2 UNKNOWN2)",
        )
        DBInterface.execute(
            binddb,
            "INSERT INTO temp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                "2021-02-12::1532",
                b"bytearray",
                "actually known",
                435,
            ],
        )
        rr = (;) # just to have the var declared
        @test_logs(
            (
                :warn,
                "Unsupported SQLite declared type UNKNOWN1, falling back to String type",
            ),
            (
                :warn,
                "Unsupported SQLite declared type UNKNOWN2, falling back to $(Int64) type",
            ),
            rr = DBInterface.execute(rowtable, binddb, "SELECT * FROM temp")
        )
        @test length(rr) == 1
        r = first(rr)
        @test typeof.(Tuple(r)) == (
            Missing,
            Int64,
            Int64,
            Float64,
            Float64,
            Int64,
            String,
            String,
            String,
            String,
            String,
            String,
            Base.CodeUnits{UInt8,String},
            String,
            Int64,
        )
    end

    @testset "Issue #158: Missing DB File" begin
        @test_throws SQLiteException SQLite.DB("nonexistentdir/not_there.db")
    end

    @testset "Issue #180, Query" begin
        param = "Hello!"
        query = DBInterface.execute(SQLite.DB(), "SELECT ?1 UNION ALL SELECT ?1", [param])
        param = "x"
        for row in query
            @test row[1] == "Hello!"
            GC.gc() # this must NOT garbage collect the "Hello!" bound value
        end

        db = SQLite.DB()
        DBInterface.execute(db, "CREATE TABLE T (a TEXT, PRIMARY KEY (a))")

        q = DBInterface.prepare(db, "INSERT INTO T VALUES(?)")
        DBInterface.execute(q, ["a"])

        SQLite.bind!(q, 1, "a")
        @test_throws SQLiteException DBInterface.execute(q)
    end

    @testset "Enable extension" begin
        db = SQLite.DB()
        @test SQLite.@OK SQLite.enable_load_extension(db)
    end

    @testset "show(DB)" begin
        io = IOBuffer()
        db = SQLite.DB()

        show(io, db)
        @test String(take!(io)) == "SQLite.DB(\":memory:\")"

        DBInterface.close!(db)
    end




    @testset "SQLite.execute()" begin
        db = SQLite.DB()
        DBInterface.execute(db, "CREATE TABLE T (x INT UNIQUE)")

        q = DBInterface.prepare(db, "INSERT INTO T VALUES(?)")
        SQLite.execute(q, (1,))
        r = DBInterface.execute(db, "SELECT * FROM T") |> columntable
        @test r[1] == [1]

        SQLite.execute(q, [2])
        r = DBInterface.execute(db, "SELECT * FROM T") |> columntable
        @test r[1] == [1, 2]

        q = DBInterface.prepare(db, "INSERT INTO T VALUES(:x)")
        SQLite.execute(q, Dict(:x => 3))
        r = DBInterface.execute(columntable, db, "SELECT * FROM T")
        @test r[1] == [1, 2, 3]

        SQLite.execute(q, x = 4)
        r = DBInterface.execute(columntable, db, "SELECT * FROM T")
        @test r[1] == [1, 2, 3, 4]

        SQLite.execute(db, "INSERT INTO T VALUES(:x)", x = 5)
        r = DBInterface.execute(columntable, db, "SELECT * FROM T")
        @test r[1] == [1, 2, 3, 4, 5]

        r = DBInterface.execute(db, strip("   SELECT * FROM T  ")) |> columntable
        @test r[1] == [1, 2, 3, 4, 5]

        SQLite.createindex!(db, "T", "x", "x_index"; unique = false)
        inds = SQLite.indices(db)
        @test last(inds.name) == "x"
        SQLite.dropindex!(db, "x")
        @test length(SQLite.indices(db).name) == 1

        cols = SQLite.columns(db, "T")
        @test cols.name == ["x"]

        @test SQLite.last_insert_rowid(db) == 5

        r = DBInterface.execute(db, "SELECT * FROM T")
        @test Tables.istable(r)
        @test Tables.rowaccess(r)
        @test Tables.rows(r) === r
        @test Base.IteratorSize(typeof(r)) == Base.SizeUnknown()
        @test eltype(r) == SQLite.Row
        row = first(r)
        SQLite.reset!(r)
        row2 = first(r)
        @test row[:x] == row2[:x]
        @test propertynames(row) == [:x]
        @test DBInterface.lastrowid(r) == 5

        r = DBInterface.execute(db, "SELECT * FROM T") |> columntable
        SQLite.load!(nothing, Tables.rows(r), db, "T2", SQLite.tableinfo(db, "T2"))
        r2 = DBInterface.execute(db, "SELECT * FROM T2") |> columntable
        @test r == r2
    end

    @testset "Escaping" begin
        @test SQLite.esc_id(["1", "2", "3"]) == "\"1\",\"2\",\"3\""
    end



    @testset "Issue #193: Throw informative error on duplicate column names" begin
        db = SQLite.DB()
        @test_throws SQLiteException SQLite.load!((a = [1, 2, 3], A = [1, 2, 3]), db)
    end


    @testset "Issue #216: Table should map by name" begin
        db = SQLite.DB()

        tbl1 = (a = [1, 2, 3], b = [4, 5, 6])
        tbl2 = (b = [7, 8, 9], a = [4, 5, 6])
        SQLite.load!(tbl1, db, "data")
        SQLite.load!(tbl2, db, "data")

        res = DBInterface.execute(db, "SELECT * FROM data") |> columntable
        expected = (a = [1, 2, 3, 4, 5, 6], b = [4, 5, 6, 7, 8, 9])
        @test res == expected
    end

    @testset "Issue #216: Table should error if names don't match" begin
        db = SQLite.DB()

        tbl1 = (a = [1, 2, 3], b = [4, 5, 6])
        SQLite.load!(tbl1, db, "data")
        tbl3 = (c = [7, 8, 9], a = [4, 5, 6])
        @test_throws SQLiteException SQLite.load!(tbl3, db, "data")
    end


    @testset "Test busy_timeout" begin
        db = SQLite.DB()
        @test SQLite.busy_timeout(db, 300) == 0
    end

    @testset "Issue #253: Ensure query column names are unique by default" begin
        db = SQLite.DB()
        res =
            DBInterface.execute(db, "select 1 as x2, 2 as x2, 3 as x2, 4 as x2_2") |>
            columntable
        @test res == (x2 = [1], x2_1 = [2], x2_2 = [3], x2_2_1 = [4])
    end

    @testset "load!() / drop!() table name escaping" begin
        db = SQLite.DB()
        tbl = (a = [1, 2, 3], b = ["a", "b", "c"])
        SQLite.load!(tbl, db, "escape 10.0%")
        r =
            DBInterface.execute(db, "SELECT * FROM $(SQLite.esc_id("escape 10.0%"))") |>
            columntable
        @test r == tbl
        SQLite.drop!(db, "escape 10.0%")
    end

    @testset "load!() column names escaping" begin
        db = SQLite.DB()
        tbl = NamedTuple{(:a, Symbol("50.0%"))}(([1, 2, 3], ["a", "b", "c"]))
        SQLite.load!(tbl, db, "escape_colnames")
        r = DBInterface.execute(db, "SELECT * FROM escape_colnames") |> columntable
        @test r == tbl
        SQLite.drop!(db, "escape_colnames")
    end

    @testset "Bool column data" begin
        db = SQLite.DB()
        tbl = (a = [true, false, false], b = [false, missing, true])
        SQLite.load!(tbl, db, "bool_data")
        r = DBInterface.execute(db, "SELECT * FROM bool_data") |> columntable
        @test isequal(r, (a = [1, 0, 0], b = [0, missing, 1]))
        SQLite.drop!(db, "bool_data")
    end

    @testset "serialization edgecases" begin
        db = SQLite.DB()
        r = DBInterface.execute(db, "SELECT zeroblob(2) as b") |> columntable
        @test first(r.b) == [0, 0]
        r = DBInterface.execute(db, "SELECT zeroblob(0) as b") |> columntable
        @test first(r.b) == []
    end

    @testset "Stmt scope" begin
        dbfile = joinpath(tempdir(), "test_stmt_scope.sqlite")
        db = SQLite.DB(dbfile)
        tbl = (a = [1, 2, 3], b = ["a", "b", "c"])

        @testset "explicit finalization by finalize_statements!(db)" begin
            SQLite.load!(tbl, db, "test_table")
            stmt = SQLite.Stmt(db, "SELECT a, b FROM test_table")
            @test SQLite.isready(stmt)
            @test SQLite.execute(stmt) == 100
            # test cannot drop the table locked by the statement
            @test_throws SQLiteException SQLite.drop!(db, "test_table")
            SQLite.finalize_statements!(db)
            @test !SQLite.isready(stmt)
            SQLite.drop!(db, "test_table")
            DBInterface.close!(stmt) # test can call close!() 2nd time
        end

        @testset "explicit finalization by close!(stmt)" begin
            SQLite.load!(tbl, db, "test_table2")
            stmt = SQLite.Stmt(db, "SELECT a, b FROM test_table2")
            @test SQLite.isready(stmt)
            @test SQLite.execute(stmt) == 100
            # test cannot drop the table locked by the statement
            @test_throws SQLiteException SQLite.drop!(db, "test_table2")
            DBInterface.close!(stmt)
            @test !SQLite.isready(stmt)
            SQLite.drop!(db, "test_table2")
            DBInterface.close!(stmt) # test can call close!() 2nd time
        end

        @testset "automatic close of implicit prepared statement" begin
            @testset "SQLite.execute() call" begin
                SQLite.load!(tbl, db, "test_table3")
                @test SQLite.execute(db, "SELECT a, b FROM test_table3") == 100
                # test can immediately drop the table, since no locks by the statement
                SQLite.drop!(db, "test_table3")
            end

            @testset "DBInterface.execute() call" begin
                SQLite.load!(tbl, db, "test_table4")
                @test SQLite.execute(db, "SELECT a, b FROM test_table4") == 100
                GC.gc() # close implicitly created statement
                # test can immediately drop the table, since no locks by the GC-ed statement
                SQLite.drop!(db, "test_table4")
            end
        end

        close(db)
        rm(dbfile)
    end

end # @testset

struct UnknownSchemaTable end

Tables.isrowtable(::Type{UnknownSchemaTable}) = true
Tables.rows(x::UnknownSchemaTable) = x
Base.length(x::UnknownSchemaTable) = 3
Base.iterate(::UnknownSchemaTable, st = 1) =
    st == 4 ? nothing : ((a = 1, b = 2 + st, c = 3 + st), st + 1)

@testset "misc" begin

    # https://github.com/JuliaDatabases/SQLite.jl/issues/259
    db = SQLite.DB()
    SQLite.load!(UnknownSchemaTable(), db, "tbl")
    tbl = DBInterface.execute(db, "select * from tbl") |> columntable
    @test tbl == (a = [1, 1, 1], b = [3, 4, 5], c = [4, 5, 6])

    # https://github.com/JuliaDatabases/SQLite.jl/issues/251
    q = DBInterface.execute(db, "select * from tbl")
    row, st = iterate(q)
    @test row.a == 1 && row.b == 3 && row.c == 4
    row2, st = iterate(q, st)
    @test_throws ArgumentError row.a

    # https://github.com/JuliaDatabases/SQLite.jl/issues/243
    db = SQLite.DB()
    DBInterface.execute(
        db,
        "create table tmp ( a INTEGER NOT NULL PRIMARY KEY, b INTEGER, c INTEGER )",
    )
    @test_throws SQLite.SQLiteException SQLite.load!(UnknownSchemaTable(), db, "tmp")
    SQLite.load!(UnknownSchemaTable(), db, "tmp"; replace = true)
    tbl = DBInterface.execute(db, "select * from tmp") |> columntable
    @test tbl == (a = [1], b = [5], c = [6])

    db = SQLite.DB()
    DBInterface.execute(db, "create table tmp ( x TEXT )")
    DBInterface.execute(db, "insert into tmp values (?)", (nothing,))
    DBInterface.execute(db, "insert into tmp values (?)", (:a,))
    tbl = DBInterface.execute(db, "select x from tmp") |> columntable
    @test isequal(tbl.x, [missing, :a])

    db = SQLite.DB()
    DBInterface.execute(db, "create table tmp (a integer, b integer, c integer)")
    stmt = DBInterface.prepare(db, "INSERT INTO tmp VALUES(?, ?, ?)")
    tbl = (a = [1, 1, 1], b = [3, 4, 5], c = [4, 5, 6])
    DBInterface.executemany(stmt, tbl)
    tbl2 = DBInterface.execute(db, "select * from tmp") |> columntable
    @test tbl == tbl2

end
