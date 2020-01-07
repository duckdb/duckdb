//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/unary_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

inline void UNARY_TYPE_CHECK(Vector &input, Vector &result) {
	if (input.type != result.type) {
		throw TypeMismatchException(input.type, result.type, "input and result types must be the same");
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
static inline void unary_function_loop(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result_data, index_t count,
                                       sel_t *__restrict sel_vector, nullmask_t nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	if (IGNORE_NULL && nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i]) {
				result_data[i] = OP::template Operation<LEFT_TYPE, RESULT_TYPE>(ldata[i]);
			}
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = OP::template Operation<LEFT_TYPE, RESULT_TYPE>(ldata[i]);
		});
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
void templated_unary_loop(Vector &input, Vector &result) {
	auto ldata = (LEFT_TYPE *)input.data;
	auto result_data = (RESULT_TYPE *)result.data;

	unary_function_loop<LEFT_TYPE, RESULT_TYPE, OP, IGNORE_NULL>(ldata, result_data, input.count, input.sel_vector,
	                                                             input.nullmask);
	result.nullmask = input.nullmask;
	result.sel_vector = input.sel_vector;
	result.count = input.count;
}

} // namespace duckdb
