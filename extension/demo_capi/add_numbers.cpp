#include "duckdb_extension.h"
#include "add_numbers.h"

DUCKDB_EXTENSION_EXTERN

// Scalar function that adds two numbers together
static void AddNumbersTogether(duckdb_function_info info, duckdb_data_chunk input, duckdb_vector output) {
	// get the total number of rows in this chunk
	idx_t input_size = duckdb_data_chunk_get_size(input);
	// extract the two input vectors
	duckdb_vector a = duckdb_data_chunk_get_vector(input, 0);
	duckdb_vector b = duckdb_data_chunk_get_vector(input, 1);
	// get the data pointers for the input vectors (both int64 as specified by the parameter types)
	auto a_data = (int64_t *)duckdb_vector_get_data(a);
	auto b_data = (int64_t *)duckdb_vector_get_data(b);
	auto result_data = (int64_t *)duckdb_vector_get_data(output);
	// get the validity vectors
	auto a_validity = duckdb_vector_get_validity(a);
	auto b_validity = duckdb_vector_get_validity(b);
	if (a_validity || b_validity) {
		// if either a_validity or b_validity is defined there might be NULL values
		duckdb_vector_ensure_validity_writable(output);
		auto result_validity = duckdb_vector_get_validity(output);
		for (idx_t row = 0; row < input_size; row++) {
			if (duckdb_validity_row_is_valid(a_validity, row) && duckdb_validity_row_is_valid(b_validity, row)) {
				// not null - do the addition
				result_data[row] = a_data[row] + b_data[row];
			} else {
				// either a or b is NULL - set the result row to NULL
				duckdb_validity_set_row_invalid(result_validity, row);
			}
		}
	} else {
		// no NULL values - iterate and do the operation directly
		for (idx_t row = 0; row < input_size; row++) {
			result_data[row] = a_data[row] + b_data[row];
		}
	}
}

// Register the AddNumbersFunction
void RegisterAddNumbersFunction(duckdb_connection connection) {
	// create a scalar function
	auto function = duckdb_create_scalar_function();
	duckdb_scalar_function_set_name(function, "add_numbers_together");

	// add a two bigint parameters
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	duckdb_scalar_function_add_parameter(function, type);
	duckdb_scalar_function_add_parameter(function, type);

	// set the return type to bigint
	duckdb_scalar_function_set_return_type(function, type);

	duckdb_destroy_logical_type(&type);

	// set up the function
	duckdb_scalar_function_set_function(function, AddNumbersTogether);

	// register and cleanup
	duckdb_register_scalar_function(connection, function);
	duckdb_destroy_scalar_function(&function);
}
