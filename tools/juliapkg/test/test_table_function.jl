
struct MyBindStruct
    count::Int64

    function MyBindStruct(count::Int64)
        return new(count)
    end
end

function MyBindFunction(info::DuckDB.BindInfo)
    DuckDB.AddResultColumn(info, "forty_two", Int64)

    parameter = DuckDB.GetParameter(info, 0)
    number = DuckDB.GetValue(parameter, Int64)
    GC.gc()
    return MyBindStruct(number)
end

mutable struct MyInitStruct
    pos::Int64

    function MyInitStruct()
        return new(0)
    end
end

function MyInitFunction(info::DuckDB.InitInfo)
	GC.gc()
    return MyInitStruct()
end

function MyMainFunction(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
	GC.gc()
    bind_info = DuckDB.GetBindInfo(info, MyBindStruct)
    init_info = DuckDB.GetInitInfo(info, MyInitStruct)

    result_array = DuckDB.GetArray(output, 0, Int64)
    count = 0
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

    DuckDB.CreateTableFunction(con, "forty_two", [Int64], MyBindFunction, MyInitFunction, MyMainFunction)
    GC.gc()

    # 3 elements
    results = DBInterface.execute(con, "SELECT * FROM forty_two(3)")
    GC.gc()

    df = DataFrame(results)
    @test names(df) == ["forty_two"]
    @test size(df, 1) == 3
    @test df.forty_two == [42, 84, 42]

    # > vsize elements
    results = DBInterface.execute(con, "SELECT COUNT(*) cnt FROM forty_two(10000)")
    GC.gc()

    df = DataFrame(results)
    @test df.cnt == [10000]

# 	@time begin
# 		results = DBInterface.execute(con, "SELECT SUM(forty_two) cnt FROM forty_two(10000000)")
# 	end
# 	df = DataFrame(results)
# 	println(df)
end
