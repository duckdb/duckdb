#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

static void concat_function(DataChunk &args, ExpressionState &state, Vector &result) {
	result.vector_type = VectorType::CONSTANT_VECTOR;
	result.nullmask.reset();
	// iterate over the vectors to count how large the final string will be
	index_t constant_lengths = 0;
	vector<index_t> result_lengths(args.size(), 0);
	for (index_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert(input.type == TypeId::VARCHAR);
		assert(input.SameCardinality(result));
		if (input.vector_type != VectorType::CONSTANT_VECTOR) {
			// non-constant vector: set the result type to a flat vector
			input.Normalify();
			auto input_data = (string_t *)input.GetData();
			result.vector_type = VectorType::FLAT_VECTOR;
			// now add the length of each vector to the result length
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i]) {
					return;
				}
				result_lengths[k] += input_data[i].GetSize();
			});
		} else {
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			auto input_data = (string_t *)input.GetData();
			constant_lengths += input_data[0].GetSize();
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = (string_t *)result.GetData();
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		// allocate an empty string of the required size
		index_t str_length = constant_lengths + result_lengths[k];
		result_data[i] = result.EmptyString(str_length);
		// we reuse the result_lengths vector to store the currently appended size
		result_lengths[k] = 0;
	});

	// now that the empty space for the strings has been allocated, perform the concatenation
	for (index_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (string_t *)input.GetData();
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			// append the constant vector to each of the strings
			auto input_ptr = input_data[0].GetData();
			auto input_len = input_data[0].GetSize();
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				memcpy(result_data[i].GetData() + result_lengths[k], input_ptr, input_len);
				result_lengths[k] += input_len;
			});
		} else {
			// standard vector
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i]) {
					return;
				}
				auto input_ptr = input_data[i].GetData();
				auto input_len = input_data[i].GetSize();
				memcpy(result_data[i].GetData() + result_lengths[k], input_ptr, input_len);
				result_lengths[k] += input_len;
			});
		}
	}
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		result_data[i].Finalize();
	});
}

static void concat_operator(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<string_t, string_t, string_t, true>(args.data[0], args.data[1], result,
		[&](string_t a, string_t b) {
			auto a_data = a.GetData();
			auto b_data = b.GetData();
			auto a_length = a.GetSize();
			auto b_length = b.GetSize();

			auto target_length = a_length + b_length;
			auto target = result.EmptyString(target_length);
			auto target_data = target.GetData();

			memcpy(target_data, a_data, a_length);
			memcpy(target_data + a_length, b_data, b_length);
			target.Finalize();
			return target;
		});
}

template<bool CONSTANT_SEP>
static void templated_concat_ws(DataChunk &args, Vector &result, string_t *sep_data) {
	vector<index_t> result_lengths(args.size(), 0);
	vector<bool> has_results(args.size(), false);
	// first figure out the lengths
	for (index_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];

		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			auto input_data = (string_t *)input.GetData();
			index_t constant_size = input_data[0].GetSize();
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				if (has_results[k]) {
					result_lengths[k] += sep_data[CONSTANT_SEP ? 0 : i].GetSize();
				}
				result_lengths[k] += constant_size;
				has_results[k] = true;
			});
		} else {
			input.Normalify();
			auto input_data = (string_t *)input.GetData();
			// standard vector
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i]) {
					return;
				}
				if (has_results[k]) {
					result_lengths[k] += sep_data[CONSTANT_SEP ? 0 : i].GetSize();
				}
				result_lengths[k] += input_data[i].GetSize();
				has_results[k] = true;
			});
		}
	}

	// first we allocate the empty strings for each of the values
	auto result_data = (string_t *)result.GetData();
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		// allocate an empty string of the required size
		result_data[i] = result.EmptyString(result_lengths[k]);
		// we reuse the result_lengths vector to store the currently appended size
		result_lengths[k] = 0;
		has_results[k] = false;
	});

	// now that the empty space for the strings has been allocated, perform the concatenation
	for (index_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (string_t *)input.GetData();
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			// append the constant vector to each of the strings
			auto input_ptr = input_data[0].GetData();
			auto input_len = input_data[0].GetSize();
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				if (has_results[k]) {
					auto sep_size = sep_data[CONSTANT_SEP ? 0 : i].GetSize();
					auto sep_ptr = sep_data[CONSTANT_SEP ? 0 : i].GetData();
					memcpy(result_data[i].GetData() + result_lengths[k], sep_ptr, sep_size);
					result_lengths[k] += sep_size;
				}
				memcpy(result_data[i].GetData() + result_lengths[k], input_ptr, input_len);
				result_lengths[k] += input_len;
				has_results[k] = true;
			});
		} else {
			// standard vector
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i]) {
					return;
				}
				if (has_results[k]) {
					auto sep_size = sep_data[CONSTANT_SEP ? 0 : i].GetSize();
					auto sep_ptr = sep_data[CONSTANT_SEP ? 0 : i].GetData();
					memcpy(result_data[i].GetData() + result_lengths[k], sep_ptr, sep_size);
					result_lengths[k] += sep_size;
				}
				auto input_ptr = input_data[i].GetData();
				auto input_len = input_data[i].GetSize();
				memcpy(result_data[i].GetData() + result_lengths[k], input_ptr, input_len);
				result_lengths[k] += input_len;
				has_results[k] = true;
			});
		}
	}
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		result_data[i].Finalize();
	});
}

static void concat_ws_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &separator = args.data[0];
	result.vector_type = VectorType::CONSTANT_VECTOR;
	for(index_t i = 0; i < args.column_count(); i++) {
		if (args.data[i].vector_type != VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::FLAT_VECTOR;
			break;
		}
	}

	if (separator.vector_type == VectorType::CONSTANT_VECTOR) {
		// constant separator
		if (separator.nullmask[0]) {
			// constant NULL as separator: return constant NULL vector
			result.vector_type = VectorType::CONSTANT_VECTOR;
			result.nullmask[0] = true;
			return;
		} else {
			// constant non-null value as separator: result has no NULL values
			result.nullmask.reset();
		}
		auto sep_data = (string_t *)separator.GetData();
		templated_concat_ws<true>(args, result, sep_data);
	} else {
		// variable separator: copy nullmask from separator
		separator.Normalify();
		result.nullmask = separator.nullmask;
		templated_concat_ws<false>(args, result, (string_t *) separator.GetData());
	}
}

void ConcatFun::RegisterFunction(BuiltinFunctions &set) {
	// the concat operator and concat function have different behavior regarding NULLs
	// this is strange but seems consistent with postgresql and mysql
	// (sqlite does not support the concat function, only the concat operator)

	// the concat operator behaves as one would expect: any NULL value present results in a NULL
	// i.e. NULL || 'hello' = NULL
	// the concat function, however, treats NULL values as an empty string
	// i.e. concat(NULL, 'hello') = 'hello'
	// concat_ws functions similarly to the concat function, except the result is NULL if the separator is NULL
	// if the separator is not NULL, however, NULL values are counted as empty string
	// there is one separate rule: there are no separators added between NULL values
	// so the NULL value and empty string are different!
	// e.g.:
	// concat_ws(',', NULL, NULL) = ""
	// concat_ws(',', '', '') = ","
	ScalarFunction concat = ScalarFunction("concat", {SQLType::VARCHAR}, SQLType::VARCHAR, concat_function);
	concat.varargs = SQLType::VARCHAR;
	set.AddFunction(concat);

	set.AddFunction(ScalarFunction("||", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_operator));

	ScalarFunction concat_ws =
	    ScalarFunction("concat_ws", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::VARCHAR, concat_ws_function);
	concat_ws.varargs = SQLType::VARCHAR;
	set.AddFunction(concat_ws);
}

} // namespace duckdb
