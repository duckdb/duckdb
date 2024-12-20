# test_scalar_udf.jl

# Define a simple scalar UDF that doubles the input value

function my_double_function(
    info::DuckDB.duckdb_function_info,
    input::DuckDB.duckdb_data_chunk,
    output::DuckDB.duckdb_vector
)
    # Convert input data chunk to DataChunk object
    input_chunk = DuckDB.DataChunk(input, false)
    n = DuckDB.get_size(input_chunk)

    # Get input vector (assuming one input parameter)
    input_vector = DuckDB.get_vector(input_chunk, 1)
    input_array = DuckDB.get_array(input_vector, Int64, n)

    # Get output vector
    output_array = DuckDB.get_array(DuckDB.Vec(output), Int64, n)

    # Perform the operation: double each input value
    for i in 1:n
        output_array[i] = input_array[i] * 2
    end
end

# Define a scalar UDF that returns NULL for odd numbers and the number itself for even numbers
function my_null_function(
    info::DuckDB.duckdb_function_info,
    input::DuckDB.duckdb_data_chunk,
    output::DuckDB.duckdb_vector
)
    # Convert input data chunk to DataChunk object
    input_chunk = DuckDB.DataChunk(input, false)
    n = DuckDB.get_size(input_chunk)

    # Get input vector
    input_vector = DuckDB.get_vector(input_chunk, 1)
    input_array = DuckDB.get_array(input_vector, Int64, n)
    validity_input = DuckDB.get_validity(input_vector)

    # Get output vector
    output_vector = DuckDB.Vec(output)
    output_array = DuckDB.get_array(output_vector, Int64, n)
    validity_output = DuckDB.get_validity(output_vector)

    # Perform the operation
    for i in 1:n
        if DuckDB.isvalid(validity_input, i)
            if input_array[i] % 2 == 0
                output_array[i] = input_array[i]
                # Validity is true by default, no need to set
            else
                # Set output as NULL
                DuckDB.setinvalid(validity_output, i)
            end
        else
            # Input is NULL, set output as NULL
            DuckDB.setinvalid(validity_output, i)
        end
    end
end

# Define a scalar UDF that always throws an error
function my_error_function(
    info::DuckDB.duckdb_function_info,
    input::DuckDB.duckdb_data_chunk,
    output::DuckDB.duckdb_vector
)
    throw(ErrorException("Runtime error in scalar function"))
end


function my_string_function_count_a(
    info::DuckDB.duckdb_function_info,
    input::DuckDB.duckdb_data_chunk,
    output::DuckDB.duckdb_vector
)



    input_chunk = DuckDB.DataChunk(input, false)
    output_vec = DuckDB.Vec(output)
    n = DuckDB.get_size(input_chunk)
    chunks = [input_chunk]
    extra_info_ptr = DuckDB.duckdb_scalar_function_get_extra_info(info)
    extra_info::DuckDB.ScalarFunction = unsafe_pointer_to_objref(extra_info_ptr)
    conversion_data = DuckDB.ColumnConversionData(chunks, 1, extra_info.logical_parameters[1], nothing)
    a_data_converted = DuckDB.DuckDB.convert_column(conversion_data)
    output_data = DuckDB.get_array(DuckDB.Vec(output), Int, n)

    # # # @info "Values" a_data b_data
    for row in 1:n
        result = count(x -> x == 'a', a_data_converted[row])
        output_data[row] = result
    end
    return nothing
end



function my_string_function_reverse_concat(
    info::DuckDB.duckdb_function_info,
    input::DuckDB.duckdb_data_chunk,
    output::DuckDB.duckdb_vector
)

    input_chunk = DuckDB.DataChunk(input, false)
    output_vec = DuckDB.Vec(output)
    n = Int64(DuckDB.get_size(input_chunk))
    chunks = [input_chunk]

    extra_info_ptr = DuckDB.duckdb_scalar_function_get_extra_info(info)

    extra_info::DuckDB.ScalarFunction = unsafe_pointer_to_objref(extra_info_ptr)
    conversion_data_a = DuckDB.ColumnConversionData(chunks, 1, extra_info.logical_parameters[1], nothing)
    conversion_data_b = DuckDB.ColumnConversionData(chunks, 2, extra_info.logical_parameters[2], nothing)

    a_data_converted = DuckDB.DuckDB.convert_column(conversion_data_a)
    b_data_converted = DuckDB.DuckDB.convert_column(conversion_data_b)

    for row in 1:n
        result = string(reverse(a_data_converted[row]), b_data_converted[row])
        DuckDB.assign_string_element(output_vec, row, result)
    end
    return nothing
end


@testset "Test custom scalar functions" begin
    # Connect to DuckDB
    db = DuckDB.DB()
    con = DuckDB.connect(db)

    # Create the test table
    DuckDB.query(con, "CREATE TABLE test_table AS SELECT i FROM range(10) t(i)")

    # Define logical type BIGINT
    type_bigint = DuckDB.duckdb_create_logical_type(DuckDB.DUCKDB_TYPE_BIGINT)

    # Test 1: Double Function
    # Create the scalar function
    f_double = DuckDB.duckdb_create_scalar_function()
    DuckDB.duckdb_scalar_function_set_name(f_double, "double_value")

    # Set parameter types
    DuckDB.duckdb_scalar_function_add_parameter(f_double, type_bigint)

    # Set return type
    DuckDB.duckdb_scalar_function_set_return_type(f_double, type_bigint)

    # Set the function
    CMyDoubleFunction = @cfunction(
        my_double_function,
        Cvoid,
        (DuckDB.duckdb_function_info, DuckDB.duckdb_data_chunk, DuckDB.duckdb_vector)
    )
    DuckDB.duckdb_scalar_function_set_function(f_double, CMyDoubleFunction)

    # Register the function
    res = DuckDB.duckdb_register_scalar_function(con.handle, f_double)
    @test res == DuckDB.DuckDBSuccess

    # Execute the function in a query
    results = DuckDB.query(con, "SELECT i, double_value(i) as doubled FROM test_table")
    df = DataFrame(results)
    @test names(df) == ["i", "doubled"]
    @test size(df, 1) == 10
    @test df.doubled == df.i .* 2

    # Test 2: Null Function
    # Create the scalar function
    f_null = DuckDB.duckdb_create_scalar_function()
    DuckDB.duckdb_scalar_function_set_name(f_null, "null_if_odd")

    # Set parameter types
    DuckDB.duckdb_scalar_function_add_parameter(f_null, type_bigint)

    # Set return type
    DuckDB.duckdb_scalar_function_set_return_type(f_null, type_bigint)

    # Set the function
    CMyNullFunction = @cfunction(
        my_null_function,
        Cvoid,
        (DuckDB.duckdb_function_info, DuckDB.duckdb_data_chunk, DuckDB.duckdb_vector)
    )
    DuckDB.duckdb_scalar_function_set_function(f_null, CMyNullFunction)

    # Register the function
    res_null = DuckDB.duckdb_register_scalar_function(con.handle, f_null)
    @test res_null == DuckDB.DuckDBSuccess

    # Execute the function in a query
    results_null = DuckDB.query(con, "SELECT i, null_if_odd(i) as value_or_null FROM test_table")
    df_null = DataFrame(results_null)
    @test names(df_null) == ["i", "value_or_null"]
    @test size(df_null, 1) == 10
    expected_values = Vector{Union{Missing, Int64}}(undef, 10)
    for idx in 1:10
        i = idx - 1  # Since i ranges from 0 to 9
        if i % 2 == 0
            expected_values[idx] = i
        else
            expected_values[idx] = missing
        end
    end
    @test all(df_null.value_or_null .=== expected_values)

    # Adjusted Test 3: Error Function
    # Create the scalar function
    f_error = DuckDB.duckdb_create_scalar_function()
    DuckDB.duckdb_scalar_function_set_name(f_error, "error_function")

    # Set parameter types
    DuckDB.duckdb_scalar_function_add_parameter(f_error, type_bigint)

    # Set return type
    DuckDB.duckdb_scalar_function_set_return_type(f_error, type_bigint)

    # Set the function
    CMyErrorFunction = @cfunction(
        my_error_function,
        Cvoid,
        (DuckDB.duckdb_function_info, DuckDB.duckdb_data_chunk, DuckDB.duckdb_vector)
    )
    DuckDB.duckdb_scalar_function_set_function(f_error, CMyErrorFunction)

    # Register the function
    res_error = DuckDB.duckdb_register_scalar_function(con.handle, f_error)
    @test res_error == DuckDB.DuckDBSuccess

    # Adjusted test to expect ErrorException
    @test_throws ErrorException DuckDB.query(con, "SELECT error_function(i) FROM test_table")

    # Clean up logical type
    DuckDB.duckdb_destroy_logical_type(type_bigint)

    # Disconnect and close
    DuckDB.disconnect(con)
    DuckDB.close(db)
end





mysum(a, b) = a + b # Dummy function
my_reverse(s) = string(reverse(s))

@testset "UDF_Macro" begin

    # Parse Expression
    expr = :(mysum(a::Int, b::String)::Int)
    func_name, func_params, return_value = DuckDB._udf_parse_function_expr(expr)
    @test func_name == :mysum
    @test func_params == [(:a, :Int), (:b, :String)]
    @test return_value == :Int

    # Build expressions
    var_names, expressions =
        DuckDB._udf_generate_conversion_expressions(func_params, :log_types, :convert, :param, :chunk)
    @test var_names == [:param_1, :param_2]
    @test expressions[1] == :(param_1 = convert(Int, log_types[1], chunk, 1))
    @test expressions[2] == :(param_2 = convert(String, log_types[2], chunk, 2))


    # Generate UDF
    db = DuckDB.DB()
    con = DuckDB.connect(db)
    fun = DuckDB.@create_scalar_function mysum(a::Int, b::Int)::Int
    #ptr = @cfunction(fun.wrapper, Cvoid, (Ptr{Cvoid},Ptr{Cvoid},Ptr{Cvoid}))

    #ptr = pointer_from_objref(mysum_udf.wrapper)
    #DuckDB.duckdb_scalar_function_set_function(mysum_udf.handle, ptr)
    DuckDB.register_scalar_function(con, fun) # Register UDF

    @test_throws ArgumentError DuckDB.register_scalar_function(con, fun) # Register UDF twice


    DuckDB.execute(con, "CREATE TABLE test1 (a INT, b INT);")
    DuckDB.execute(con, "INSERT INTO test1 VALUES ('1', '2'), ('3','4'), ('5', '6')")
    result = DuckDB.execute(con, "SELECT mysum(a, b) as result FROM test1") |> DataFrame
    @test result.result == [3, 7, 11]

end

@testset "UDF Macro Various Types" begin
    import Dates
    db = DuckDB.DB()
    con = DuckDB.connect(db)

    my_reverse_inner = (s) -> ("Inner:" * string(reverse(s)))
    fun_is_weekend = (d) -> Dates.dayofweek(d) in (6, 7)
    date_2020 = (x) -> Dates.Date(2020, 1, 1) + Dates.Day(x) # Dummy function

    my_and(a, b) = a && b
    my_int_add(a, b) = a + b
    my_mixed_add(a::Int, b::Float64) = a + b

    df_numbers =
        DataFrame(a = rand(1:100, 30), b = rand(1:100, 30), c = rand(30), d = rand(Bool, 30), e = rand(Bool, 30))
    df_strings = DataFrame(a = ["hello", "world", "julia", "duckdb", "🦆DB"])
    t = Date(2020, 1, 1):Date(2020, 12, 31)
    df_dates = DataFrame(t = t, k = 1:length(t), is_weekend = fun_is_weekend.(t))


    DuckDB.register_table(con, df_strings, "test_strings")
    DuckDB.register_table(con, df_dates, "test_dates")
    DuckDB.register_table(con, df_numbers, "test_numbers")


    # Register UDFs
    fun_string = DuckDB.@create_scalar_function my_reverse(s::String)::String (s) -> my_reverse_inner(s)
    DuckDB.register_scalar_function(con, fun_string) # Register UDF

    fun_date = DuckDB.@create_scalar_function is_weekend(d::Date)::Bool fun_is_weekend
    fun_date2 = DuckDB.@create_scalar_function date_2020(x::Int)::Date date_2020
    DuckDB.register_scalar_function(con, fun_date) # Register UDF
    DuckDB.register_scalar_function(con, fun_date2) # Register UDF

    fun_and = DuckDB.@create_scalar_function my_and(a::Bool, b::Bool)::Bool my_and
    fun_int_add = DuckDB.@create_scalar_function my_int_add(a::Int, b::Int)::Int my_int_add
    fun_mixed_add = DuckDB.@create_scalar_function my_mixed_add(a::Int, b::Float64)::Float64 my_mixed_add
    DuckDB.register_scalar_function(con, fun_and)
    DuckDB.register_scalar_function(con, fun_int_add)
    DuckDB.register_scalar_function(con, fun_mixed_add)

    result1 = DuckDB.execute(con, "SELECT my_reverse(a) as result FROM test_strings") |> DataFrame
    @test result1.result == my_reverse_inner.(df_strings.a)


    result2_1 = DuckDB.execute(con, "SELECT is_weekend(t) as result FROM test_dates") |> DataFrame
    @test result2_1.result == fun_is_weekend.(df_dates.t)
    result2_2 = DuckDB.execute(con, "SELECT date_2020(k) as result FROM test_dates") |> DataFrame
    @test result2_2.result == date_2020.(df_dates.k)

    result3 = DuckDB.execute(con, "SELECT my_and(d, e) as result FROM test_numbers") |> DataFrame
    @test result3.result == my_and.(df_numbers.d, df_numbers.e)


    result4 = DuckDB.execute(con, "SELECT my_int_add(a, b) as result FROM test_numbers") |> DataFrame
    @test result4.result == my_int_add.(df_numbers.a, df_numbers.b)

    result5 = DuckDB.execute(con, "SELECT my_mixed_add(a, c) as result FROM test_numbers") |> DataFrame
    @test result5.result == my_mixed_add.(df_numbers.a, df_numbers.c)

end

@testset "UDF Macro Exception" begin

    f_error = function (a)
        if iseven(a)
            throw(ArgumentError("Even number"))
        else
            return a + 1
        end
    end


    db = DuckDB.DB()
    con = DuckDB.connect(db)

    fun_error = DuckDB.@create_scalar_function f_error(a::Int)::Int f_error
    DuckDB.register_scalar_function(con, fun_error) # Register UDF

    df = DataFrame(a = 1:10)
    DuckDB.register_table(con, df, "test1")

    @test_throws "Even number" result = DuckDB.execute(con, "SELECT f_error(a) as result FROM test1") |> DataFrame
end

@testset "UDF Macro Missing Values" begin

    f_add = (a, b) -> a + b
    db = DuckDB.DB()
    con = DuckDB.connect(db)

    fun = DuckDB.@create_scalar_function f_add(a::Int, b::Int)::Int f_add
    DuckDB.register_scalar_function(con, fun)

    df = DataFrame(a = [1, missing, 3], b = [missing, 2, 3])
    DuckDB.register_table(con, df, "test1")

    result = DuckDB.execute(con, "SELECT f_add(a, b) as result FROM test1") |> DataFrame
    @test isequal(result.result, [missing, missing, 6])
end

@testset "UDF Macro Benchmark" begin
    # Check if the generated UDF is comparable to pure Julia or DuckDB expressions
    # 
    # Currently UDFs takes about as much time as Julia/DuckDB expressions
    #   - The evaluation of the wrapper takes around 20% of the execution time
    #   - slow calls are setindex! and getindex
    #   - table_scan_func is the slowest call 


    db = DuckDB.DB()
    con = DuckDB.connect(db)
    fun_int = DuckDB.@create_scalar_function mysum(a::Int, b::Int)::Int
    fun_float = DuckDB.@create_scalar_function mysum_f(a::Float64, b::Float64)::Float64 mysum

    DuckDB.register_scalar_function(con, fun_int) # Register UDF
    DuckDB.register_scalar_function(con, fun_float) # Register UDF

    N = 10_000_000
    df = DataFrame(a = 1:N, b = 1:N, c = rand(N), d = rand(N))

    DuckDB.register_table(con, df, "test1")

    # Precompile functions
    precompile(mysum, (Int, Int))
    precompile(mysum, (Float64, Float64))
    DuckDB.execute(con, "SELECT mysum(a, b) as result FROM test1")
    DuckDB.execute(con, "SELECT mysum_f(c, d) as result FROM test1")

    # INTEGER Benchmark

    t1 = @elapsed result_exp = df.a .+ df.b
    t2 = @elapsed result = DuckDB.execute(con, "SELECT mysum(a, b) as result FROM test1")
    t3 = @elapsed result2 = DuckDB.execute(con, "SELECT a + b as result FROM test1")
    @test DataFrame(result).result == result_exp
    # Prints:
    # Benchmark Int: Julia Expression: 0.092947083, UDF: 0.078665125, DDB: 0.065306042
    @info "Benchmark Int: Julia Expression: $t1, UDF: $t2, DDB: $t3"


    # FLOAT Benchmark
    t1 = @elapsed result_exp = df.c .+ df.d
    t2 = @elapsed result = DuckDB.execute(con, "SELECT mysum_f(c, d) as result FROM test1")
    t3 = @elapsed result2 = DuckDB.execute(con, "SELECT c + d as result FROM test1")
    @test DataFrame(result).result ≈ result_exp atol = 1e-6
    # Prints:
    # Benchmark Float: Julia Expression: 0.090409625, UDF: 0.080781, DDB: 0.054156167
    @info "Benchmark Float: Julia Expression: $t1, UDF: $t2, DDB: $t3"
end
