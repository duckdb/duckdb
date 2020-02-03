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
	assert(result.type == TypeId::BOOL);

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

bool VectorOperations::HasNull(Vector &input) {
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		return input.nullmask[0];
	} else {
		input.Normalify();
		if (!input.nullmask.any()) {
			return false;
		}
		bool has_null = false;
		VectorOperations::Exec(input, [&](index_t i, index_t k) {
			if (input.nullmask[i]) {
				has_null = true;
			}
		});
		return has_null;
	}
}
