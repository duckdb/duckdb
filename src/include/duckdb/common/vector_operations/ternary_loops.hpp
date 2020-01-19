//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL, bool A_CONSTANT,
          bool B_CONSTANT, bool C_CONSTANT>
static inline void ternary_function_loop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
                                         RESULT_TYPE *__restrict result_data, index_t count,
                                         sel_t *__restrict sel_vector, nullmask_t &nullmask) {
	if (!A_CONSTANT) {
		ASSERT_RESTRICT(adata, adata + count, result_data, result_data + count);
	}
	if (!B_CONSTANT) {
		ASSERT_RESTRICT(bdata, bdata + count, result_data, result_data + count);
	}
	if (!C_CONSTANT) {
		ASSERT_RESTRICT(cdata, cdata + count, result_data, result_data + count);
	}
	if (IGNORE_NULL && nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i]) {
				result_data[i] =
				    OP::Operation(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i]);
			}
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			result_data[i] =
			    OP::Operation(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i]);
		});
	}
}

template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class OP, bool IGNORE_NULL = false>
void templated_ternary_loop(Vector &a, Vector &b, Vector &c, Vector &result) {
	auto adata = (A_TYPE *)a.GetData();
	auto bdata = (B_TYPE *)b.GetData();
	auto cdata = (C_TYPE *)c.GetData();
	auto result_data = (RESULT_TYPE *)result.GetData();

	if (a.vector_type == VectorType::CONSTANT_VECTOR) {
		if (b.vector_type == VectorType::CONSTANT_VECTOR) {
			result.sel_vector = c.sel_vector;
			result.count = c.count;
			if (c.vector_type == VectorType::CONSTANT_VECTOR) {
				// ABC constant, result is constant vector
				result.vector_type = VectorType::CONSTANT_VECTOR;
				if (a.nullmask[0] || b.nullmask[0] || c.nullmask[0]) {
					result.nullmask[0] = true;
				} else {
					result_data[0] = OP::Operation(adata[0], bdata[0], cdata[0]);
				}
			} else {
				// AB constant
				result.vector_type = VectorType::FLAT_VECTOR;
				if (a.nullmask[0] || b.nullmask[0]) {
					result.nullmask.set();
					return;
				}
				result.nullmask = c.nullmask;
				ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, true, true, false>(
					adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
			}
		} else if (c.vector_type == VectorType::CONSTANT_VECTOR) {
			// AC constant
			result.vector_type = VectorType::FLAT_VECTOR;
			result.sel_vector = b.sel_vector;
			result.count = b.count;
			if (a.nullmask[0] || c.nullmask[0]) {
				result.nullmask.set();
				return;
			}
			result.nullmask = b.nullmask;
			ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, true, false, true>(
			    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
		} else {
			// A constant
			result.vector_type = VectorType::FLAT_VECTOR;
			result.sel_vector = b.sel_vector;
			result.count = b.count;
			result.nullmask = a.nullmask | c.nullmask;
			ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, true, false, false>(
			    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
		}
	} else if (b.vector_type == VectorType::CONSTANT_VECTOR) {
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = a.sel_vector;
		result.count = a.count;
		if (b.nullmask[0]) {
			result.nullmask.set();
			return;
		}
		if (c.vector_type == VectorType::CONSTANT_VECTOR) {
			// BC constant
			if (c.nullmask[0]) {
				result.nullmask.set();
				return;
			}
			result.nullmask = a.nullmask;
			ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, true, true>(
			    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
		} else {
			// B constant
			result.nullmask = a.nullmask | c.nullmask;
			ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, true, false>(
			    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
		}
	} else if (c.vector_type == VectorType::CONSTANT_VECTOR) {
		// C constant
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = a.sel_vector;
		result.count = a.count;
		if (c.nullmask[0]) {
			result.nullmask.set();
			return;
		}
		// OR the nullmask of A and B
		result.nullmask = a.nullmask | b.nullmask;
		ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, false, true>(
		    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
	} else {
		// no constants
		result.vector_type = VectorType::FLAT_VECTOR;
		result.sel_vector = a.sel_vector;
		result.count = a.count;
		result.nullmask = a.nullmask | b.nullmask | c.nullmask;
		ternary_function_loop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP, IGNORE_NULL, false, false, false>(
		    adata, bdata, cdata, result_data, result.count, result.sel_vector, result.nullmask);
	}
}

} // namespace duckdb
