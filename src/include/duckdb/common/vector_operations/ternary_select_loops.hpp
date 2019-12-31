//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_select_loops.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool A_CONSTANT, bool B_CONSTANT, bool C_CONSTANT>
static inline index_t ternary_select_loop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
                                          sel_t *__restrict result, index_t count, sel_t *__restrict sel_vector,
                                          nullmask_t &nullmask) {
	index_t result_count = 0;
	if (nullmask.any()) {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (!nullmask[i] &&
			    OP::Operation(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i])) {
				result[result_count++] = i;
			}
		});
	} else {
		VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
			if (OP::Operation(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i])) {
				result[result_count++] = i;
			}
		});
	}
	return result_count;
}

template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
index_t templated_ternary_select(Vector &a, Vector &b, Vector &c, sel_t result[]) {
	auto adata = (A_TYPE *)a.data;
	auto bdata = (B_TYPE *)b.data;
	auto cdata = (C_TYPE *)c.data;

	if (a.IsConstant()) {
		if (a.nullmask[0]) {
			return 0;
		}
		if (b.IsConstant()) {
			if (b.nullmask[0]) {
				return 0;
			}
			if (c.IsConstant()) {
				// early out in case everything is a constant
				// in this case we don't want to edit the selection vector in the result at all
				// hence we return 0 or 1 based on the condition immediately
				if (c.nullmask[0] || !OP::Operation(adata[0], bdata[0], cdata[0])) {
					return 0;
				} else {
					return 1;
				}
			}
			// AB constant
			return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, true, true, false>(
			    adata, bdata, cdata, result, c.count, c.sel_vector, c.nullmask);
		} else if (c.IsConstant()) {
			if (c.nullmask[0]) {
				return 0;
			}
			// AC constant
			return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, true, false, true>(
			    adata, bdata, cdata, result, b.count, b.sel_vector, b.nullmask);
		} else {
			// A constant
			auto nullmask = b.nullmask | c.nullmask;
			return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, true, false, false>(adata, bdata, cdata, result,
			                                                                           b.count, b.sel_vector, nullmask);
		}
	} else if (b.IsConstant()) {
		if (b.nullmask[0]) {
			return 0;
		}
		if (c.IsConstant()) {
			if (c.nullmask[0]) {
				return 0;
			}
			// BC constant
			return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, false, true, true>(
			    adata, bdata, cdata, result, a.count, a.sel_vector, a.nullmask);
		} else {
			// B constant
			auto nullmask = a.nullmask | c.nullmask;
			return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, false, true, false>(adata, bdata, cdata, result,
			                                                                           a.count, a.sel_vector, nullmask);
		}
	} else if (c.IsConstant()) {
		if (c.nullmask[0]) {
			return 0;
		}
		// C constant
		auto nullmask = a.nullmask | b.nullmask;
		return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, false, false, true>(adata, bdata, cdata, result, a.count,
		                                                                           a.sel_vector, nullmask);
	} else {
		// no constants
		auto nullmask = a.nullmask | b.nullmask | c.nullmask;
		return ternary_select_loop<A_TYPE, B_TYPE, C_TYPE, OP, false, false, false>(adata, bdata, cdata, result,
		                                                                            a.count, a.sel_vector, nullmask);
	}
}

} // namespace duckdb
