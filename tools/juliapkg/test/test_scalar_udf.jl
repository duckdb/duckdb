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
