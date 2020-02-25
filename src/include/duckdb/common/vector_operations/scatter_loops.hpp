//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/scatter_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class T, class OP>
static inline void scatter_loop_constant(T constant, T **__restrict destination, idx_t count,
                                         sel_t *__restrict sel_vector) {
	VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
		if (!IsNullValue<T>(*destination[i])) {
			*destination[i] = OP::Operation(constant, *destination[i]);
		} else {
			*destination[i] = constant;
		}
	});
}

template <class T, class OP>
static inline void scatter_loop(T *__restrict ldata, T **__restrict destination, idx_t count,
                                sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	ASSERT_RESTRICT(ldata, ldata + count, destination, destination + count);
	VectorOperations::Exec(sel_vector, count, [&](idx_t i, idx_t k) {
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
	assert(dest.SameCardinality(source));
	auto ldata = (T *)source.GetData();
	auto destination = (T **)dest.GetData();
	if (source.vector_type == VectorType::CONSTANT_VECTOR) {
		// special case: source is a constant
		if (source.nullmask[0]) {
			// source is NULL, do nothing
			return;
		}

		auto constant = ldata[0];
		scatter_loop_constant<T, OP>(constant, destination, dest.size(), dest.sel_vector());
	} else {
		// source and dest are equal-length vectors
		scatter_loop<T, OP>(ldata, destination, dest.size(), dest.sel_vector(), source.nullmask);
	}
}

} // namespace duckdb
