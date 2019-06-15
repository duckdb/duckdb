//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/vector_operations/unary_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

inline void UNARY_TYPE_CHECK(Vector &input, Vector &result) {
	if (input.type != result.type) {
		throw TypeMismatchException(input.type, result.type, "input and result types must be the same");
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void unary_loop_function(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, index_t count,
                                       sel_t *__restrict sel_vector) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) { result_data[i] = OP::Operation(ldata[i]); });
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP> void templated_unary_loop(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;

	unary_loop_function<LEFT_TYPE, RESULT_TYPE, OP>(ldata, result_data, input.count, input.sel_vector);
	result.nullmask = input.nullmask;
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void unary_loop_process_null_function(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data,
                                                    index_t count, sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { result_data[i] = OP::Operation(ldata[i], nullmask[i]); });
	} else {
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { result_data[i] = OP::Operation(ldata[i], false); });
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
void templated_unary_loop_process_null(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;

	result.nullmask.reset();
	unary_loop_process_null_function<LEFT_TYPE, RESULT_TYPE, OP>(ldata, result_data, input.count, input.sel_vector,
	                                                             input.nullmask);
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}
} // namespace duckdb
