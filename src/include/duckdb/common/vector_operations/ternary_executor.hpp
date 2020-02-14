//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <functional>

namespace duckdb {

struct TernaryExecutor {
private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN, bool IGNORE_NULL, bool A_CONSTANT,
	          bool B_CONSTANT, bool C_CONSTANT>
	static inline void ExecuteLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               RESULT_TYPE *__restrict result_data, index_t count, sel_t *__restrict sel_vector,
	                               nullmask_t &nullmask, FUN fun) {
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
					    fun(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i]);
				}
			});
		} else {
			VectorOperations::Exec(sel_vector, count, [&](index_t i, index_t k) {
				result_data[i] = fun(adata[A_CONSTANT ? 0 : i], bdata[B_CONSTANT ? 0 : i], cdata[C_CONSTANT ? 0 : i]);
			});
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN, bool IGNORE_NULL, bool A_CONSTANT>
	static inline void ExecuteA(Vector &a, Vector &b, Vector &c, Vector &result, FUN fun) {
		if (b.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteAB<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, A_CONSTANT, true>(a, b, c, result, fun);
		} else {
			b.Normalify();
			ExecuteAB<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, A_CONSTANT, false>(a, b, c, result, fun);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN, bool IGNORE_NULL, bool A_CONSTANT,
	          bool B_CONSTANT>
	static inline void ExecuteAB(Vector &a, Vector &b, Vector &c, Vector &result, FUN fun) {
		if (c.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteABC<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, A_CONSTANT, B_CONSTANT, true>(
			    a, b, c, result, fun);
		} else {
			c.Normalify();
			ExecuteABC<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, A_CONSTANT, B_CONSTANT, false>(
			    a, b, c, result, fun);
		}
	}

	template <bool A_CONSTANT, bool B_CONSTANT, bool C_CONSTANT>
	static void SetNullmask(Vector &a, Vector &b, Vector &c, nullmask_t &result_nullmask) {
		if (A_CONSTANT) {
			if (B_CONSTANT) {
				// AB constant
				result_nullmask = c.nullmask;
			} else {
				if (C_CONSTANT) {
					// AC constant
					result_nullmask = b.nullmask;
				} else {
					// A constant
					result_nullmask = b.nullmask | c.nullmask;
				}
			}
		} else {
			if (B_CONSTANT) {
				if (C_CONSTANT) {
					// BC constant
					result_nullmask = a.nullmask;
				} else {
					// B constant
					result_nullmask = a.nullmask | c.nullmask;
				}
			} else if (C_CONSTANT) {
				// C constant
				result_nullmask = a.nullmask | b.nullmask;
			} else {
				// nothing constant
				result_nullmask = a.nullmask | b.nullmask | c.nullmask;
			}
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN, bool IGNORE_NULL, bool A_CONSTANT,
	          bool B_CONSTANT, bool C_CONSTANT>
	static inline void ExecuteABC(Vector &a, Vector &b, Vector &c, Vector &result, FUN fun) {
		auto adata = (A_TYPE *)a.GetData();
		auto bdata = (B_TYPE *)b.GetData();
		auto cdata = (C_TYPE *)c.GetData();
		auto result_data = (RESULT_TYPE *)result.GetData();

		if ((A_CONSTANT && a.nullmask[0]) || (B_CONSTANT && b.nullmask[0]) || (C_CONSTANT && c.nullmask[0])) {
			// if any constant NULL exists the result is a constant NULL
			result.vector_type = VectorType::CONSTANT_VECTOR;
			result.nullmask[0] = true;
			return;
		}
		if (A_CONSTANT && B_CONSTANT && C_CONSTANT) {
			// everything is constant, result is constant
			result.vector_type = VectorType::CONSTANT_VECTOR;
			result.nullmask[0] = false;
			assert(!a.nullmask[0] && !b.nullmask[0] && !c.nullmask[0]);
			result_data[0] = fun(adata[0], bdata[0], cdata[0]);
			return;
		}

		// not everything is a constant: the result is a flat vector
		result.vector_type = VectorType::FLAT_VECTOR;
		// we have to create the NULL mask by ORing together the different nullmasks of non-constant vectors
		SetNullmask<A_CONSTANT, B_CONSTANT, C_CONSTANT>(a, b, c, result.nullmask);

		// finally we perform the actual loop over the data operation
		ExecuteLoop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, A_CONSTANT, B_CONSTANT, C_CONSTANT>(
		    adata, bdata, cdata, result_data, result.size(), result.sel_vector(), result.nullmask, fun);
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, bool IGNORE_NULL = false,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &result, FUN fun) {
		assert(a.SameCardinality(b) && a.SameCardinality(c) && a.SameCardinality(result));
		if (a.vector_type == VectorType::CONSTANT_VECTOR) {
			ExecuteA<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, true>(a, b, c, result, fun);
		} else {
			a.Normalify();
			ExecuteA<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUN, IGNORE_NULL, false>(a, b, c, result, fun);
		}
	}

private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool A_CONSTANT, bool B_CONSTANT, bool C_CONSTANT>
	static inline index_t SelectLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
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

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool A_CONSTANT>
	static inline index_t SelectA(Vector &a, Vector &b, Vector &c, sel_t result[]) {
		if (b.vector_type == VectorType::CONSTANT_VECTOR) {
			if (b.nullmask[0]) {
				return 0;
			}
			return SelectAB<A_TYPE, B_TYPE, C_TYPE, OP, A_CONSTANT, true>(a, b, c, result);
		} else {
			b.Normalify();
			return SelectAB<A_TYPE, B_TYPE, C_TYPE, OP, A_CONSTANT, false>(a, b, c, result);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool A_CONSTANT, bool B_CONSTANT>
	static inline index_t SelectAB(Vector &a, Vector &b, Vector &c, sel_t result[]) {
		if (c.vector_type == VectorType::CONSTANT_VECTOR) {
			if (c.nullmask[0]) {
				return 0;
			}
			return SelectABC<A_TYPE, B_TYPE, C_TYPE, OP, A_CONSTANT, B_CONSTANT, true>(a, b, c, result);
		} else {
			c.Normalify();
			return SelectABC<A_TYPE, B_TYPE, C_TYPE, OP, A_CONSTANT, B_CONSTANT, false>(a, b, c, result);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool A_CONSTANT, bool B_CONSTANT, bool C_CONSTANT>
	static inline index_t SelectABC(Vector &a, Vector &b, Vector &c, sel_t result[]) {
		auto adata = (A_TYPE *)a.GetData();
		auto bdata = (B_TYPE *)b.GetData();
		auto cdata = (C_TYPE *)c.GetData();

		if (A_CONSTANT && B_CONSTANT && C_CONSTANT) {
			assert(!a.nullmask[0] && !b.nullmask[0] && !c.nullmask[0]);
			// everything is constant, perform the operation once on all the counts and return all rows or zero rows
			// based on that
			if (OP::Operation(adata[0], bdata[0], cdata[0])) {
				return a.size();
			} else {
				return 0;
			}
		}

		// get the nullmask of the final result
		nullmask_t nullmask;
		SetNullmask<A_CONSTANT, B_CONSTANT, C_CONSTANT>(a, b, c, nullmask);

		// finally perform the select loop to get the count and the final result
		return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, A_CONSTANT, B_CONSTANT, C_CONSTANT>(
		    adata, bdata, cdata, result, a.size(), a.sel_vector(), nullmask);
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static index_t Select(Vector &a, Vector &b, Vector &c, sel_t result[]) {
		assert(a.SameCardinality(b) && a.SameCardinality(c));
		if (a.vector_type == VectorType::CONSTANT_VECTOR) {
			if (a.nullmask[0]) {
				return 0;
			}
			return SelectA<A_TYPE, B_TYPE, C_TYPE, OP, true>(a, b, c, result);
		} else {
			a.Normalify();
			return SelectA<A_TYPE, B_TYPE, C_TYPE, OP, false>(a, b, c, result);
		}
	}
};

} // namespace duckdb
