//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_select_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {


template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool IGNORE_NULL, int LEFT_MULTIPLIER, int RIGHT_MULTIPLIER>
static inline index_t binary_select_loop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                  sel_t *__restrict result, index_t count,
                                                  sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	index_t result_count = 0;
	if (IGNORE_NULL || nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i] && OP::Operation(ldata[LEFT_MULTIPLIER * i], rdata[RIGHT_MULTIPLIER * i])) {
				result[result_count++] = i;
			}
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (OP::Operation(ldata[LEFT_MULTIPLIER * i], rdata[RIGHT_MULTIPLIER * i])) {
				result[result_count++] = i;
			}
		});
	}
	return result_count;
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool IGNORE_NULL>
static inline index_t binary_select_left_constant(LEFT_TYPE ldata, RIGHT_TYPE *__restrict rdata,
                                                  sel_t *__restrict result, index_t count,
                                                  sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	return binary_select_loop<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL, 0, 1>(&ldata, rdata, result, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool IGNORE_NULL>
static inline index_t binary_select_right_constant(LEFT_TYPE *__restrict ldata, RIGHT_TYPE rdata,
                                                       sel_t *__restrict result, index_t count,
                                                       sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	return binary_select_loop<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL, 1, 0>(ldata, &rdata, result, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool IGNORE_NULL>
static inline index_t binary_select_array(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                              sel_t *__restrict result, index_t count,
                                              sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	return binary_select_loop<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL, 1, 1>(ldata, rdata, result, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class OP, bool IGNORE_NULL = false>
index_t templated_binary_select(Vector &left, Vector &right, sel_t result[]) {
	auto ldata = (LEFT_TYPE *)left.data;
	auto rdata = (RIGHT_TYPE *)right.data;

	if (left.IsConstant()) {
		if (right.IsConstant()) {
			// early out in case both sides are constant
			// in this case we don't want to edit the selection vector in the result at all
			// hence we return 0 or 1 based on the condition immediately
			if (left.nullmask[0] || right.nullmask[0] || !OP::Operation(ldata[0], rdata[0])) {
				return 0;
			} else {
				return 1;
			}
		}

		if (left.nullmask[0]) {
			// left side is constant NULL; no results
			return 0;
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			return binary_select_left_constant<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL>(
				ldata[0], rdata, result, right.count, right.sel_vector, right.nullmask);
		}
	} else if (right.IsConstant()) {
		if (right.nullmask[0]) {
			// right side is constant NULL, no results
			return 0;
		} else {
			return binary_select_right_constant<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL>(
				ldata, rdata[0], result, left.count, left.sel_vector, left.nullmask);
		}
	} else {
		assert(left.count == right.count);
		assert(left.sel_vector == right.sel_vector);
		// OR nullmasks together
		auto nullmask = left.nullmask | right.nullmask;
		return binary_select_array<LEFT_TYPE, RIGHT_TYPE, OP, IGNORE_NULL>(ldata, rdata, result, left.count, left.sel_vector, nullmask);
	}
}

}
