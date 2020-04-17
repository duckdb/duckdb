//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/filter_combiner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/expression.hpp"

#include "duckdb/storage/data_table.hpp"
#include <functional>

namespace duckdb {

enum class ValueComparisonResult { PRUNE_LEFT, PRUNE_RIGHT, UNSATISFIABLE_CONDITION, PRUNE_NOTHING };
enum class FilterResult { UNSATISFIABLE, SUCCESS, UNSUPPORTED };

//! The FilterCombiner combines several filters and generates a logically equivalent set that is more efficient
//! Amongst others:
//! (1) it prunes obsolete filter conditions: i.e. [X > 5 and X > 7] => [X > 7]
//! (2) it generates new filters for expressions in the same equivalence set: i.e. [X = Y and X = 500] => [Y = 500]
//! (3) it prunes branches that have unsatisfiable filters: i.e. [X = 5 AND X > 6] => FALSE, prune branch
class FilterCombiner {
public:
	struct ExpressionValueInformation {
		Value constant;
		ExpressionType comparison_type;
	};

	FilterResult AddFilter(unique_ptr<Expression> expr);

	void GenerateFilters(std::function<void(unique_ptr<Expression> filter)> callback);
	bool HasFilters();
	vector<TableFilter> GenerateTableScanFilters(std::function<void(unique_ptr<Expression> filter)> callback,
	                                             vector<idx_t> &column_ids);

private:
	FilterResult AddFilter(Expression *expr);

	Expression *GetNode(Expression *expr);
	idx_t GetEquivalenceSet(Expression *expr);
	FilterResult AddConstantComparison(vector<ExpressionValueInformation> &info_list, ExpressionValueInformation info);

	vector<unique_ptr<Expression>> remaining_filters;

	expression_map_t<unique_ptr<Expression>> stored_expressions;
	unordered_map<Expression *, idx_t> equivalence_set_map;
	unordered_map<idx_t, vector<ExpressionValueInformation>> constant_values;
	unordered_map<idx_t, vector<Expression *>> equivalence_map;
	idx_t set_index = 0;
};

} // namespace duckdb
