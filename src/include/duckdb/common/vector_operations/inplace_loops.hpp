//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/inplace_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class LEFT_TYPE, class RESULT_TYPE, class OP, bool INPUT_CONSTANT>
static inline void templated_inplace_loop_function(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data,
                                               index_t count, sel_t *__restrict sel_vector) {
	if (!INPUT_CONSTANT) {
		ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	}
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		OP::Operation(result_data[i], ldata[INPUT_CONSTANT ? 0 : i]);
	});
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP> void templated_inplace_loop(Vector &input, Vector &result) {
	assert(input.type == result.type);
	assert(result.vector_type == VectorType::FLAT_VECTOR);

	auto result_data = (RESULT_TYPE *)result.GetData();
	if (input.vector_type == VectorType::CONSTANT_VECTOR) {
		auto ldata = (LEFT_TYPE *)input.GetData();
		// constant vector
		if (input.nullmask[0]) {
			result.nullmask.set();
		} else {
			templated_inplace_loop_function<LEFT_TYPE, RESULT_TYPE, OP, true>(ldata, result_data, result.count, result.sel_vector);
		}
	} else {
		input.Normalify();
		auto ldata = (LEFT_TYPE *)input.GetData();
		assert(input.count == result.count);
		assert(result.sel_vector == input.sel_vector);
		// OR nullmasks together
		result.nullmask = input.nullmask | result.nullmask;
		templated_inplace_loop_function<LEFT_TYPE, RESULT_TYPE, OP, false>(ldata, result_data, input.count, input.sel_vector);
	}
}

} // namespace duckdb
