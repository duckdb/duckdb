//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/constant_filter_combiner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_result.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class ValueComparisonResult { PRUNE_LEFT, PRUNE_RIGHT, UNSATISFIABLE_CONDITION, PRUNE_NOTHING };

struct ConstantComparison {
	Value constant;
	ExpressionType comparison_type;
};

//! Reduces constant comparisons on one target, independent of logical or scan-filter representation.
class ConstantFilterCombiner {
public:
	static bool IsSupportedComparison(ExpressionType type);
	static bool IsLowerBound(ExpressionType type) {
		return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	}
	static bool IsUpperBound(ExpressionType type) {
		return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
	}
	static ValueComparisonResult Compare(const ConstantComparison &left, const ConstantComparison &right);

	static FilterResult AddComparison(vector<ConstantComparison> &comparisons, ConstantComparison comparison) {
		return AddComparison(comparisons, std::move(comparison),
		                     [](ConstantComparison &, const ConstantComparison &) {});
	}

	//! MERGE transfers any caller-owned metadata from a removed comparison to its replacement.
	template <class T, class MERGE>
	static FilterResult AddComparison(vector<T> &comparisons, T comparison, MERGE merge) {
		if (comparison.constant.IsNull()) {
			return FilterResult::UNSATISFIABLE;
		}
		for (idx_t i = 0; i < comparisons.size(); i++) {
			switch (Compare(comparisons[i], comparison)) {
			case ValueComparisonResult::PRUNE_LEFT:
				merge(comparison, comparisons[i]);
				comparisons.erase_at(i--);
				break;
			case ValueComparisonResult::PRUNE_RIGHT:
				merge(comparisons[i], comparison);
				return FilterResult::SUCCESS;
			case ValueComparisonResult::UNSATISFIABLE_CONDITION:
				comparisons.push_back(std::move(comparison));
				return FilterResult::UNSATISFIABLE;
			default:
				break;
			}
		}
		comparisons.push_back(std::move(comparison));
		return FilterResult::SUCCESS;
	}
};

} // namespace duckdb
