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
	assert(input.SameCardinality(result));
	assert(result.type == TypeId::BOOL);

	result.vector_type = input.vector_type;
	result.nullmask.reset();

	auto result_data = (bool *)result.GetData();
	VectorOperations::Exec(input, [&](index_t i, index_t k) {
		result_data[i] = INVERSE ? !input.nullmask[i] : input.nullmask[i];
	});
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

index_t VectorOperations::NotNullSelVector(Vector &vector, sel_t *not_null_vector, sel_t *&result_assignment,
                                  sel_t *null_vector) {
	vector.Normalify();
	if (vector.nullmask.any()) {
		uint64_t result_count = 0, null_count = 0;
		VectorOperations::Exec(vector, [&](uint64_t i, uint64_t k) {
			if (!vector.nullmask[i]) {
				not_null_vector[result_count++] = i;
			} else if (null_vector) {
				null_vector[null_count++] = i;
			}
		});
		result_assignment = not_null_vector;
		return result_count;
	} else {
		result_assignment = vector.sel_vector();
		return vector.size();
	}
}
