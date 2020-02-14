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
	// iterate over the vectors to check if the result is a constant vector or not
	for (index_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert(input.type == TypeId::VARCHAR);
		if (input.vector_type != VectorType::CONSTANT_VECTOR) {
			// regular vector: set the result type to a flat vector
			assert(input.vector_type == VectorType::FLAT_VECTOR);
			assert(input.SameCardinality(result));
			result.vector_type = VectorType::FLAT_VECTOR;
		}
	}

	// now perform the actual concatenation
	vector<string> results(args.size());
	for (index_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (const char **)input.GetData();
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			VectorOperations::Exec(result, [&](index_t i, index_t k) { results[k] += input_data[0]; });
		} else {
			// standard vector
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i]) {
					return;
				}
				results[k] += input_data[i];
			});
		}
	}
	// now write the final result to the result vector and add the strings to the heap
	auto result_data = (const char **)result.GetData();
	VectorOperations::Exec(result, [&](index_t i, index_t k) { result_data[i] = result.AddString(results[k]); });
}

static void concat_operator(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<const char *, const char *, const char *, true>(args.data[0], args.data[1], result,
	                                                                        [&](const char *a, const char *b) {
		                                                                        string concat = string(a) + b;
		                                                                        return result.AddString(concat);
	                                                                        });
}

static void concat_ws_constant_sep(DataChunk &args, Vector &result, vector<string> &results, string sep) {
	// now perform the actual concatenation
	vector<bool> has_results(args.size(), false);
	for (index_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (const char **)input.GetData();
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				if (has_results[k]) {
					results[k] += sep;
				}
				results[k] += input_data[0];
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
					results[k] += sep;
				}
				results[k] += input_data[i];
				has_results[k] = true;
			});
		}
	}
}

static void concat_ws_variable_sep(DataChunk &args, Vector &result, vector<string> &results, Vector &separator) {
	auto sep_data = (const char **)separator.GetData();
	// now perform the actual concatenation
	vector<bool> has_results(result.size(), false);
	for (index_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (const char **)input.GetData();
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector
			if (input.nullmask[0]) {
				// constant null, skip
				continue;
			}
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				if (separator.nullmask[i]) {
					return;
				}
				if (has_results[k]) {
					results[k] += sep_data[i];
				}
				results[k] += input_data[0];
				has_results[k] = true;
			});
		} else {
			// standard vector
			VectorOperations::Exec(result, [&](index_t i, index_t k) {
				// ignore null entries
				if (input.nullmask[i] || separator.nullmask[i]) {
					return;
				}
				if (has_results[k]) {
					results[k] += sep_data[i];
				}
				results[k] += input_data[i];
				has_results[k] = true;
			});
		}
	}
}

static void concat_ws_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &separator = args.data[0];
	auto sep_data = (const char **)separator.GetData();

	result.vector_type = VectorType::CONSTANT_VECTOR;
	// iterate over the vectors to check the result vector type
	for (index_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert(input.type == TypeId::VARCHAR);
		if (input.vector_type != VectorType::CONSTANT_VECTOR) {
			// regular vector: set the result type to a flat vector
			assert(input.vector_type == VectorType::FLAT_VECTOR);
			assert(input.SameCardinality(result));
			result.vector_type = VectorType::FLAT_VECTOR;
		}
	}

	// check if we are dealing with a constant separator or a variable separator
	vector<string> results(result.size());
	if (args.data[0].vector_type == VectorType::CONSTANT_VECTOR) {
		if (separator.nullmask[0]) {
			// constant NULL as separator: return constant NULL vector
			result.vector_type = VectorType::CONSTANT_VECTOR;
			result.nullmask[0] = true;
			return;
		} else {
			// constant non-null value as separator: result has no NULL values
			result.nullmask.reset();
		}
		concat_ws_constant_sep(args, result, results, sep_data[0]);
	} else {
		// variable-length separator: copy nullmask
		result.nullmask = separator.nullmask;
		concat_ws_variable_sep(args, result, results, separator);
	}

	// now write the final result to the result vector and add the strings to the heap
	auto result_data = (const char **)result.GetData();
	VectorOperations::Exec(result, [&](index_t i, index_t k) { result_data[i] = result.AddString(results[k]); });
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
