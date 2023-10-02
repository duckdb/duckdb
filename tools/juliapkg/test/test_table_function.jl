# test_table_function.jl

struct MyBindStruct
    count::Int64

    function MyBindStruct(count::Int64)
        return new(count)
    end
end

function my_bind_function(info::DuckDB.BindInfo)
    DuckDB.add_result_column(info, "forty_two", Int64)

    parameter = DuckDB.get_parameter(info, 0)
    number = DuckDB.getvalue(parameter, Int64)
    return MyBindStruct(number)
end

mutable struct MyInitStruct
    pos::Int64

    function MyInitStruct()
        return new(0)
    end
end

function my_init_function(info::DuckDB.InitInfo)
    return MyInitStruct()
end

function my_main_function_print(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.get_bind_info(info, MyBindStruct)
    init_info = DuckDB.get_init_info(info, MyInitStruct)

    result_array = DuckDB.get_array(output, 1, Int64)
    count = 0
    for i in 1:(DuckDB.VECTOR_SIZE)
        if init_info.pos >= bind_info.count
            break
        end
        result_array[count + 1] = init_info.pos % 2 == 0 ? 42 : 84
        # We print within the table function to test behavior with synchronous API calls in Julia table functions
        println(result_array[count + 1])
        count += 1
        init_info.pos += 1
    end

    DuckDB.set_size(output, count)
    return
end

function my_main_function(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.get_bind_info(info, MyBindStruct)
    init_info = DuckDB.get_init_info(info, MyInitStruct)

    result_array = DuckDB.get_array(output, 1, Int64)
    count = 0
    for i in 1:(DuckDB.VECTOR_SIZE)
        if init_info.pos >= bind_info.count
            break
        end
        result_array[count + 1] = init_info.pos % 2 == 0 ? 42 : 84
        count += 1
        init_info.pos += 1
    end

    DuckDB.set_size(output, count)
    return
end

function my_main_function_nulls(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    bind_info = DuckDB.get_bind_info(info, MyBindStruct)
    init_info = DuckDB.get_init_info(info, MyInitStruct)

    result_array = DuckDB.get_array(output, 1, Int64)
    validity = DuckDB.get_validity(output, 1)
    count = 0
    for i in 1:(DuckDB.VECTOR_SIZE)
        if init_info.pos >= bind_info.count
            break
        end
        if init_info.pos % 2 == 0
            result_array[count + 1] = 42
        else
            DuckDB.setinvalid(validity, count + 1)
        end
        count += 1
        init_info.pos += 1
    end

    DuckDB.set_size(output, count)
    return
end

@testset "Test custom table functions that produce IO" begin
    con = DBInterface.connect(DuckDB.DB)

    DuckDB.create_table_function(
        con,
        "forty_two_print",
        [Int64],
        my_bind_function,
        my_init_function,
        my_main_function_print
    )
    GC.gc()

    # 3 elements
    results = DBInterface.execute(con, "SELECT * FROM forty_two_print(3)")
    GC.gc()

    df = DataFrame(results)
    @test names(df) == ["forty_two"]
    @test size(df, 1) == 3
    @test df.forty_two == [42, 84, 42]

    # > vsize elements
    results = DBInterface.execute(con, "SELECT COUNT(*) cnt FROM forty_two_print(10000)")
    GC.gc()

    df = DataFrame(results)
    @test df.cnt == [10000]

    # 	@time begin
    # 		results = DBInterface.execute(con, "SELECT SUM(forty_two) cnt FROM forty_two(10000000)")
    # 	end
    # 	df = DataFrame(results)
    # 	println(df)
end

@testset "Test custom table functions" begin
    con = DBInterface.connect(DuckDB.DB)

    DuckDB.create_table_function(con, "forty_two", [Int64], my_bind_function, my_init_function, my_main_function)
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

    # return null values from a table function
    DuckDB.create_table_function(
        con,
        "forty_two_nulls",
        [Int64],
        my_bind_function,
        my_init_function,
        my_main_function_nulls
    )
    results = DBInterface.execute(con, "SELECT COUNT(*) total_cnt, COUNT(forty_two) cnt FROM forty_two_nulls(10000)")
    df = DataFrame(results)
    @test df.total_cnt == [10000]
    @test df.cnt == [5000]

    # 	@time begin
    # 		results = DBInterface.execute(con, "SELECT SUM(forty_two) cnt FROM forty_two_nulls(10000000)")
    # 	end
    # 	df = DataFrame(results)
    # 	println(df)
end

function my_bind_error_function(info::DuckDB.BindInfo)
    throw("bind error")
end

function my_init_error_function(info::DuckDB.InitInfo)
    throw("init error")
end

function my_main_error_function(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    throw("runtime error")
end

@testset "Test table function errors" begin
    con = DBInterface.connect(DuckDB.DB)

    DuckDB.create_table_function(
        con,
        "bind_error_function",
        [Int64],
        my_bind_error_function,
        my_init_function,
        my_main_function
    )
    DuckDB.create_table_function(
        con,
        "init_error_function",
        [Int64],
        my_bind_function,
        my_init_error_function,
        my_main_function
    )
    DuckDB.create_table_function(
        con,
        "main_error_function",
        [Int64],
        my_bind_function,
        my_init_function,
        my_main_error_function
    )

    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM bind_error_function(3)")
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM init_error_function(3)")
    @test_throws DuckDB.QueryException DBInterface.execute(con, "SELECT * FROM main_error_function(3)")
end
