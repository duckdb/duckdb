
mutable struct MyBindStruct
    count::Int64

    function MyBindStruct(count::Int64)
        return new(count)
    end
end

function MyBindFunction(info::DuckDB.BindInfo)
    DuckDB.AddResultColumn(info, "forty_two", DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT))

    parameter = DuckDB.GetParameter(info, 0)
    number = DuckDB.GetValue(parameter, Int64)
    return MyBindStruct(number)
end

mutable struct MyInitStruct
    pos::Int64

    function MyInitStruct()
        return new(0)
    end
end

function MyInitFunction(info::DuckDB.InitInfo)
    return MyInitStruct()
end

function MyMainFunction(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info::MyBindStruct = DuckDB.GetBindInfo(info)
    init_info::MyInitStruct = DuckDB.GetInitInfo(info)

    result_array::Vector{Int64} = DuckDB.GetArray(output, 0, Int64)
    count::Int64 = 0
    for i in 1:(DuckDB.VECTOR_SIZE)
        if init_info.pos >= bind_info.count
            break
        end
        result_array[count + 1] = init_info.pos % 2 == 0 ? 42 : 84
        count += 1
        init_info.pos += 1
    end

    DuckDB.SetSize(output, count)
    return
end

@testset "Test custom table functions" begin
    con = DBInterface.connect(DuckDB.DB)

    types = [DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT)]
    DuckDB.CreateTableFunction(con.main_connection, "forty_two", types, MyBindFunction, MyInitFunction, MyMainFunction)

    GC.enable(false)

    # 3 elements
    results = DBInterface.execute(con, "SELECT * FROM forty_two(3)")

    df = DataFrame(results)
    @test names(df) == ["forty_two"]
    @test size(df, 1) == 3
    @test df.forty_two == [42, 84, 42]

    # > vsize elements
    results = DBInterface.execute(con, "SELECT COUNT(*) cnt FROM forty_two(10000)")

    df = DataFrame(results)
    @test df.cnt == [10000]

    GC.enable(true)
    GC.gc()

    #     @time begin
    #         results = DBInterface.execute(con, "SELECT SUM(forty_two) cnt FROM forty_two(10000000)")
    #     end
    #     df = DataFrame(results)
    #     println(df)
end
