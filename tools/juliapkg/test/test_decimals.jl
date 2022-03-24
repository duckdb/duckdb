# test_decimals.jl


@testset "Test decimal support" begin
    con = DBInterface.connect(DuckDB.DB)

    results = DBInterface.execute(
        con,
        "SELECT 42.3::DECIMAL(4,1) a, 4923.3::DECIMAL(9,1) b, 421.423::DECIMAL(18,3) c, 129481294.3392::DECIMAL(38,4) d"
    )

    # convert to DataFrame
    df = DataFrame(results)
    @test names(df) == ["a", "b", "c", "d"]
    @test size(df, 1) == 1
    @test df.a == [42.3]
    @test df.b == [4923.3]
    @test df.c == [421.423]
    @test df.d == [129481294.3392]

    DBInterface.close!(con)
end

# test returning decimals in a table function
function my_bind_function(info::DuckDB.BindInfo)
    DuckDB.add_result_column(info, "a", FixedDecimal{Int16, 0})
    DuckDB.add_result_column(info, "b", FixedDecimal{Int32, 1})
    DuckDB.add_result_column(info, "c", FixedDecimal{Int64, 2})
    DuckDB.add_result_column(info, "d", FixedDecimal{Int128, 3})
    return missing
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

function my_main_function(info::DuckDB.FunctionInfo, output::DuckDB.DataChunk)
    init_info = DuckDB.get_init_info(info, MyInitStruct)

    a_array = DuckDB.get_array(output, 1, Int16)
    b_array = DuckDB.get_array(output, 2, Int32)
    c_array = DuckDB.get_array(output, 3, Int64)
    d_array = DuckDB.get_array(output, 4, Int128)
    count = 0
    multiplier = 1
    for i in 1:(DuckDB.VECTOR_SIZE)
        if init_info.pos >= 3
            break
        end
        a_array[count + 1] = 42 * multiplier
        b_array[count + 1] = 42 * multiplier
        c_array[count + 1] = 42 * multiplier
        d_array[count + 1] = 42 * multiplier
        count += 1
        init_info.pos += 1
        multiplier *= 10
    end

    DuckDB.set_size(output, count)
    return
end

@testset "Test returning decimals from a table functions" begin
    con = DBInterface.connect(DuckDB.DB)

    arguments::Vector{DataType} = Vector()
    DuckDB.create_table_function(con, "my_function", arguments, my_bind_function, my_init_function, my_main_function)
    GC.gc()

    # 3 elements
    results = DBInterface.execute(con, "SELECT * FROM my_function()")
    GC.gc()

    df = DataFrame(results)
    @test names(df) == ["a", "b", "c", "d"]
    @test size(df, 1) == 3
    @test df.a == [42, 420, 4200]
    @test df.b == [4.2, 42, 420]
    @test df.c == [0.42, 4.2, 42]
    @test df.d == [0.042, 0.42, 4.2]
end
