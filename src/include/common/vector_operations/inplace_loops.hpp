//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/vector_operations/inplace_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

inline void INPLACE_TYPE_CHECK(Vector &left, Vector &result) {
	if (left.type != result.type) {
		throw TypeMismatchException(left.type, result.type, "input and result types must be the same");
	}
	if (!left.IsConstant() && left.count != result.count) {
		throw Exception("Cardinality exception: left and result cannot have "
		                "different cardinalities");
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void inplace_loop_function_constant(LEFT_TYPE ldata, RESULT_TYPE *__restrict result_data, index_t count,
                                                  sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) { OP::Operation(result_data[i], ldata); });
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void inplace_loop_function_array(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data,
                                               index_t count, sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) { OP::Operation(result_data[i], ldata[i]); });
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP> void templated_inplace_loop(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;
	if (input.IsConstant()) {
		if (input.nullmask[0]) {
			result.nullmask.set();
		} else {
			LEFT_TYPE constant = ldata[0];
			inplace_loop_function_constant<LEFT_TYPE, RESULT_TYPE, OP>(constant, result_data, result.count,
			                                                           result.sel_vector);
		}
	} else {
		// OR nullmasks together
		result.nullmask = input.nullmask | result.nullmask;
		assert(result.sel_vector == input.sel_vector);
		inplace_loop_function_array<LEFT_TYPE, RESULT_TYPE, OP>(ldata, result_data, input.count, input.sel_vector);
	}
}

} // namespace duckdb
