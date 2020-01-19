//===--------------------------------------------------------------------===//
// null_operators.cpp
// Description: This file contains the implementation of the
// IS NULL/NOT IS NULL operators
//===--------------------------------------------------------------------===//

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

template <bool INVERSE> void is_null_loop(Vector &input, Vector &result) {
	assert(result.type == TypeId::BOOLEAN);

	result.vector_type = input.vector_type;
	result.nullmask.reset();

	auto result_data = (bool *)result.GetData();
	VectorOperations::Exec(input.sel_vector, input.count, [&](index_t i, index_t k) {
		result_data[i] = INVERSE ? !input.nullmask[i] : input.nullmask[i];
	});
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}

void VectorOperations::IsNotNull(Vector &input, Vector &result) {
	is_null_loop<true>(input, result);
}

void VectorOperations::IsNull(Vector &input, Vector &result) {
	is_null_loop<false>(input, result);
}
