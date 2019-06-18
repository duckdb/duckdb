//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/vector_operations/scatter_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/types/vector.hpp"
#include "common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T, class OP>
static inline void scatter_loop_constant(T constant, T **__restrict destination, index_t count,
                                         sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		if (!IsNullValue<T>(*destination[i])) {
			*destination[i] = OP::Operation(constant, *destination[i]);
		} else {
			*destination[i] = constant;
		}
	});
}

template <class T, class OP>
static inline void scatter_loop(T *__restrict ldata, T **__restrict destination, index_t count,
                                sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, destination, destination + count);
	VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
		if (!nullmask[i]) {
			if (!IsNullValue<T>(*destination[i])) {
				*destination[i] = OP::Operation(ldata[i], *destination[i]);
			} else {
				*destination[i] = ldata[i];
			}
		}
	});
}

template <class T, class OP> void scatter_templated_loop(Vector &source, Vector &dest) {
	auto ldata = (T *)source.data;
	auto destination = (T **)dest.data;
	if (source.IsConstant()) {
		// special case: source is a constant
		if (source.nullmask[0]) {
			// source is NULL, do nothing
			return;
		}

		auto constant = ldata[0];
		scatter_loop_constant<T, OP>(constant, destination, dest.count, dest.sel_vector);
	} else {
		// source and dest are equal-length vectors
		assert(dest.sel_vector == source.sel_vector);
		scatter_loop<T, OP>(ldata, destination, dest.count, dest.sel_vector, source.nullmask);
	}
}

} // namespace duckdb
