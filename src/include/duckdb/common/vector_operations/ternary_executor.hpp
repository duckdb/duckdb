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
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUN fun) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		VariadicExecutor::Execute<RESULT_TYPE, A_TYPE, B_TYPE, C_TYPE>(inputs, result, count, fun);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class OP>
	static void ExecuteStandard(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		VariadicExecutor::ExecuteStandard<RESULT_TYPE, OP, A_TYPE, B_TYPE, C_TYPE>(inputs, result, count);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static idx_t Select(Vector &a, Vector &b, Vector &c, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		std::array<VariadicExecutor::VectorRef, 3> inputs = {{a, b, c}};
		return VariadicExecutor::Select<OP, A_TYPE, B_TYPE, C_TYPE>(inputs, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb
