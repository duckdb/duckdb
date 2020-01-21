#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <string.h>

using namespace std;

namespace duckdb {

static void concat_function(DataChunk &args, ExpressionState &state, Vector &result) {
	result.vector_type = VectorType::CONSTANT_VECTOR;
	result.nullmask.reset();
	result.sel_vector = args.data[0].sel_vector;
	result.count = args.data[0].count;
	// iterate over the vectors to set the nullmasks
	for (index_t col_idx = 0; col_idx < args.column_count; col_idx++) {
		auto &input = args.data[col_idx];
		assert(input.type == TypeId::VARCHAR);
		if (input.vector_type == VectorType::CONSTANT_VECTOR) {
			// constant vector: check for NULL
			if (input.nullmask[0]) {
				// constant NULL, entire result is null
				result.vector_type = VectorType::CONSTANT_VECTOR;
				result.nullmask[0] = true;
				return;
			}
		} else {
			// regular vector: merge the null mask and set the result type to a flat vector
			assert(input.vector_type == VectorType::FLAT_VECTOR);
			result.vector_type = VectorType::FLAT_VECTOR;
			result.sel_vector = input.sel_vector;
			result.count = input.count;
			result.nullmask |= input.nullmask;
		}
	}


	// now perform the actual concatenation
	vector<string> results(result.count);
	for(index_t col_idx = 0; col_idx < args.column_count; col_idx++) {
		auto &input = args.data[col_idx];
		auto input_data = (const char **) input.GetData();
		bool is_constant = input.vector_type == VectorType::CONSTANT_VECTOR;
		assert(input.vector_type == VectorType::FLAT_VECTOR || input.vector_type == VectorType::CONSTANT_VECTOR);
		// loop over the vector and concat to all results
		VectorOperations::Exec(result, [&](index_t i, index_t k) {
			if (result.nullmask[i]) {
				return;
			}
			results[k] += input_data[is_constant ? 0 : i];
		});
	}
	// now write the final result to the result vector and add the strings to the heap
	auto result_data = (const char **)result.GetData();
	VectorOperations::Exec(result, [&](index_t i, index_t k) {
		result_data[i] = result.string_heap.AddString(results[k]);
	});
}

void ConcatFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction concat =
	    ScalarFunction("concat", {SQLType::VARCHAR}, SQLType::VARCHAR, concat_function);
	concat.varargs = SQLType::VARCHAR;
	set.AddFunction(concat);
	concat.name = "||";
	set.AddFunction(concat);
}

} // namespace duckdb
