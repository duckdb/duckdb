//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/string_comparison.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

template <class OP, StringComparisonMode MODE>
struct StringOrderingOperator {
	template <class T>
	static bool Operation(const T &left, const T &right) {
		using COMPARE = string_t::StringComparisonOperators;
		if constexpr (std::is_same_v<OP, GreaterThan>) {
			return COMPARE::GreaterThan<MODE>(left, right);
		} else if constexpr (std::is_same_v<OP, LessThan>) {
			return COMPARE::GreaterThan<MODE>(right, left);
		} else if constexpr (std::is_same_v<OP, GreaterThanEquals>) {
			return !COMPARE::GreaterThan<MODE>(right, left);
		} else {
			static_assert(std::is_same_v<OP, LessThanEquals>, "Expected an ordering comparison");
			return !COMPARE::GreaterThan<MODE>(left, right);
		}
	}
};

struct StringComparison {
	template <class OP>
	static constexpr bool IsOrdering() {
		return std::is_same_v<OP, GreaterThan> || std::is_same_v<OP, LessThan> ||
		       std::is_same_v<OP, GreaterThanEquals> || std::is_same_v<OP, LessThanEquals>;
	}

	static bool HasInlinedConstant(const Vector &left, const Vector &right);
	static StringComparisonMode SelectMode(const Vector &left, const Vector &right, idx_t count);
	static StringComparisonMode SelectMode(const UnifiedVectorFormat &input, const string_t &constant,
	                                       const SelectionVector &sel, idx_t count);

	template <class OP, class FUNC>
	static auto Dispatch(StringComparisonMode mode, FUNC &&function) {
		switch (mode) {
		case StringComparisonMode::GENERIC:
			return function(StringOrderingOperator<OP, StringComparisonMode::GENERIC>());
		case StringComparisonMode::PREFIX:
			return function(StringOrderingOperator<OP, StringComparisonMode::PREFIX>());
		default:
			return function(OP());
		}
	}

	template <class OP>
	static void Execute(const Vector &left, const Vector &right, Vector &result) {
		if constexpr (InlinedStringEqualityOperator<OP>::ENABLED) {
			if (HasInlinedConstant(left, right)) {
				using INLINED_OP = typename InlinedStringEqualityOperator<OP>::OPERATOR;
				BinaryExecutor::Execute<string_t, string_t, bool, INLINED_OP>(left, right, result);
				return;
			}
		} else if constexpr (IsOrdering<OP>()) {
			if (left.size() == right.size()) {
				return Dispatch<OP>(SelectMode(left, right, left.size()), [&](auto op) {
					BinaryExecutor::Execute<string_t, string_t, bool, decltype(op)>(left, right, result);
				});
			}
		}
		BinaryExecutor::Execute<string_t, string_t, bool, OP>(left, right, result);
	}

	template <class OP>
	static idx_t Select(const Vector &left, const Vector &right, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if constexpr (InlinedStringEqualityOperator<OP>::ENABLED) {
			if (HasInlinedConstant(left, right)) {
				using INLINED_OP = typename InlinedStringEqualityOperator<OP>::OPERATOR;
				return BinaryExecutor::Select<string_t, string_t, INLINED_OP>(left, right, sel, count, true_sel,
				                                                              false_sel);
			}
		} else if constexpr (IsOrdering<OP>()) {
			return Dispatch<OP>(SelectMode(left, right, count), [&](auto op) {
				return BinaryExecutor::Select<string_t, string_t, decltype(op)>(left, right, sel, count, true_sel,
				                                                                false_sel);
			});
		}
		return BinaryExecutor::Select<string_t, string_t, OP>(left, right, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb
