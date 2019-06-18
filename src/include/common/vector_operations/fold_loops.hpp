//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/vector_operations/fold_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class LEFT_TYPE, class RESULT_TYPE, class OP>
static inline void fold_loop_function(LEFT_TYPE *__restrict ldata, RESULT_TYPE *__restrict result, index_t count,
                                      sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result, result + 1);
	if (nullmask.any()) {
		// skip null values in the operation
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i]) {
				*result = OP::Operation(ldata[i], *result);
			}
		});
	} else {
		// quick path: no NULL values
		VectorOperations::Exec(sel_vector, count,
		                       [&](index_t i, index_t k) { *result = OP::Operation(ldata[i], *result); });
	}
}

template <class LEFT_TYPE, class RESULT_TYPE, class OP> void templated_unary_fold(Vector &input, RESULT_TYPE *result) {
	auto ldata = (LEFT_TYPE *)input.data;
	fold_loop_function<LEFT_TYPE, RESULT_TYPE, OP>(ldata, result, input.count, input.sel_vector, input.nullmask);
}

} // namespace duckdb
