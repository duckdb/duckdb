//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_operations/variadic_executor.hpp"

#include <functional>

namespace duckdb {

struct TernaryExecutor {
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(const Vector &a, const Vector &b, const Vector &c, Vector &result, FUN fun) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		VariadicExecutor::Execute<RESULT_TYPE, A_TYPE, B_TYPE, C_TYPE>(inputs, result, fun);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(const Vector &a, const Vector &b, const Vector &c, Vector &result) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		VariadicExecutor::ExecuteStandard<RESULT_TYPE, OP, A_TYPE, B_TYPE, C_TYPE>(inputs, result);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static idx_t Select(const Vector &a, const Vector &b, const Vector &c, optional_ptr<const SelectionVector> sel,
	                    idx_t count, optional_ptr<SelectionVector> true_sel, optional_ptr<SelectionVector> false_sel) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		return VariadicExecutor::Select<OP, A_TYPE, B_TYPE, C_TYPE>(inputs, sel.get(), count, true_sel.get(),
		                                                            false_sel.get());
	}

	//===--------------------------------------------------------------------===//
	// Deprecated overloads (count parameter removed - use count-free versions)
	//===--------------------------------------------------------------------===//
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	[[deprecated("count parameter is deprecated; call Execute without count instead")]] static void
	Execute(const Vector &a, const Vector &b, const Vector &c, Vector &result, idx_t count, FUN fun) {
		if (count != a.size()) {
			throw InternalException("TernaryExecutor::Execute: count (%llu) does not match vector size (%llu)", count,
			                        a.size());
		}
		Execute<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE>(a, b, c, result, fun);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class OP>
	[[deprecated("count parameter is deprecated; call ExecuteStandard without count instead")]] static void
	ExecuteStandard(const Vector &a, const Vector &b, const Vector &c, Vector &result, idx_t count) {
		if (count != a.size()) {
			throw InternalException("TernaryExecutor::ExecuteStandard: count (%llu) does not match vector size (%llu)",
			                        count, a.size());
		}
		ExecuteStandard<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, OP>(a, b, c, result);
	}
};

} // namespace duckdb
