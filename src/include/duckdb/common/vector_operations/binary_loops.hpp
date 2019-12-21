//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/binary_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

inline void BINARY_TYPE_CHECK(Vector &left, Vector &right, Vector &result) {
	if (left.type != right.type) {
		throw TypeMismatchException(left.type, right.type, "left and right types must be the same");
	}
	if (left.type != result.type) {
		throw TypeMismatchException(left.type, result.type, "result type must be the same as input types");
	}
	if (!left.IsConstant() && !right.IsConstant() && left.count != right.count) {
		throw Exception("Cardinality exception: left and right cannot have "
		                "different cardinalities");
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
static inline void binary_function_loop(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                                  RESULT_TYPE *__restrict result_data, index_t count,
                                                  sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	if (IGNORE_NULL && nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i]) {
				result_data[i] = OP::Operation(ldata[LEFT_CONSTANT ? 0 : i], rdata[RIGHT_CONSTANT ? 0 : i]);
			}
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] = OP::Operation(ldata[LEFT_CONSTANT ? 0 : i], rdata[RIGHT_CONSTANT ? 0 : i]);
		});
	}
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL>
static inline void binary_loop_left_constant(LEFT_TYPE ldata, RIGHT_TYPE *__restrict rdata,
                                                      RESULT_TYPE *__restrict result_data, index_t count,
                                                      sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
	binary_function_loop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL, true, false>(&ldata, rdata, result_data, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL>
static inline void binary_loop_right_constant(LEFT_TYPE *__restrict ldata, RIGHT_TYPE rdata,
                                                       RESULT_TYPE *__restrict result_data, index_t count,
                                                       sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	binary_function_loop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, true>(ldata, &rdata, result_data, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL>
static inline void binary_loop_array(LEFT_TYPE *__restrict ldata, RIGHT_TYPE *__restrict rdata,
                                              RESULT_TYPE *__restrict result_data, index_t count,
                                              sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, result_data, result_data + count);
	ASSERT_RESTRICT(rdata, rdata + count, result_data, result_data + count);
	binary_function_loop<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, false>(ldata, rdata, result_data, count, sel_vector, nullmask);
}

template <class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
void templated_binary_loop(Vector &left, Vector &right, Vector &result) {
	auto ldata = (LEFT_TYPE *)left.data;
	auto rdata = (RIGHT_TYPE *)right.data;
	auto result_data = (RESULT_TYPE *)result.data;

	if (left.IsConstant()) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask
			result.nullmask = right.nullmask;
			binary_loop_left_constant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL>(
				ldata[0], rdata, result_data, right.count, right.sel_vector, result.nullmask);
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		if (right.nullmask[0]) {
			// right side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			result.nullmask = left.nullmask;
			binary_loop_right_constant<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL>(
				ldata, rdata[0], result_data, left.count, left.sel_vector, result.nullmask);
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		assert(left.count == right.count);
		assert(left.sel_vector == right.sel_vector);
		// OR nullmasks together
		result.nullmask = left.nullmask | right.nullmask;
		binary_loop_array<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, OP, IGNORE_NULL>(ldata, rdata, result_data, left.count, left.sel_vector, result.nullmask);
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	}
}

//===--------------------------------------------------------------------===//
// Division & Modulo
//===--------------------------------------------------------------------===//
// to handle (division by zero -> NULL and modulo with 0 -> NULL) we have a separate function
template <class T, class OP> void templated_divmod_loop(Vector &left, Vector &right, Vector &result) {
	auto ldata = (T *)left.data;
	auto rdata = (T *)right.data;
	auto result_data = (T *)result.data;

	if (left.IsConstant()) {
		if (left.nullmask[0]) {
			// left side is constant NULL, set everything to NULL
			result.nullmask.set();
		} else {
			// left side is normal constant, use right nullmask and do
			// computation
			T constant = ldata[0];
			result.nullmask = right.nullmask;
			VectorOperations::Exec(right, [&](index_t i, index_t k) {
				if (rdata[i] == 0) {
					result.nullmask[i] = true;
				} else {
					result_data[i] = OP::Operation(constant, rdata[i]);
				}
			});
		}
		result.sel_vector = right.sel_vector;
		result.count = right.count;
	} else if (right.IsConstant()) {
		T constant = rdata[0];
		if (right.nullmask[0] || constant == 0) {
			// right side is constant NULL OR division by constant 0, set
			// everything to NULL
			result.nullmask.set();
		} else {
			// right side is normal constant, use left nullmask and do
			// computation
			result.nullmask = left.nullmask;
			binary_loop_right_constant<T, T, T, OP, false>(ldata, constant, result_data, left.count, left.sel_vector, result.nullmask);
		}
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	} else {
		assert(left.count == right.count);
		// OR nullmasks together
		result.nullmask = left.nullmask | right.nullmask;
		assert(left.sel_vector == right.sel_vector);
		VectorOperations::Exec(left, [&](index_t i, index_t k) {
			if (rdata[i] == 0) {
				result.nullmask[i] = true;
			} else {
				result_data[i] = OP::Operation(ldata[i], rdata[i]);
			}
		});
		result.sel_vector = left.sel_vector;
		result.count = left.count;
	}
}

} // namespace duckdb
