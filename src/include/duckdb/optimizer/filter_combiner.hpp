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
#include "duckdb/planner/filter/constant_filter_combiner.hpp"
#include "duckdb/planner/joinside.hpp"

#include "duckdb/storage/data_table.hpp"
#include <functional>
#include <map>

namespace duckdb {
class BoundColumnRefExpression;
class LogicalGet;
class Optimizer;

enum class FilterPushdownResult { NO_PUSHDOWN, PUSHED_DOWN_PARTIALLY, PUSHED_DOWN_FULLY };

//! The FilterCombiner combines several filters and generates a logically equivalent set that is more efficient
//! Amongst others:
//! (1) it prunes obsolete filter conditions: i.e. [X > 5 and X > 7] => [X > 7]
//! (2) it generates new filters for expressions in the same equivalence set: i.e. [X = Y and X = 500] => [Y = 500]
//! (3) it prunes branches that have unsatisfiable filters: i.e. [X = 5 AND X > 6] => FALSE, prune branch
class FilterCombiner {
public:
	explicit FilterCombiner(ClientContext &context);
	explicit FilterCombiner(Optimizer &optimizer);

	ClientContext &context;

public:
	using ExpressionValueInformation = ConstantComparison;

	FilterResult AddFilter(unique_ptr<Expression> expr);
	//! Splits on AND and adds each conjunct
	FilterResult AddConjuncts(unique_ptr<Expression> expr);

	//! Returns whether or not a set of integral values is a dense range (i.e. 1, 2, 3, 4, 5)
	//! If this returns true - this sorts "in_list" as a side-effect
	static bool IsDenseRange(vector<Value> &in_list);
	static bool ContainsNull(vector<Value> &in_list);

	void GenerateFilters(const std::function<void(unique_ptr<Expression> filter)> &callback);
	bool HasFilters();
	//! Whether expression equality preserves the identity of constants within this expression.
	static bool CanMatchExpression(const Expression &expr);
	//! A predicate that can be copied without changing evaluation effects or conflating matched operands.
	static bool CanCopyFilter(const Expression &filter);
	//! A safe predicate with the same truth value for keys equal under NOT DISTINCT FROM.
	static bool IsKeyConstraint(const Expression &filter, const vector<reference<const Expression>> &keys);
	//! Append a linear set of equalities and key constraints in projection output bindings.
	//! lookup_output maps an exact input expression to an existing output column; source filters are retained.
	static void GenerateProjectionFilters(
	    ClientContext &context, const vector<unique_ptr<Expression>> &filters,
	    const std::function<unique_ptr<BoundColumnRefExpression>(const Expression &)> &lookup_output,
	    vector<unique_ptr<Expression>> &result);
	//! Remove exact duplicate, non-throwing, non-volatile predicates.
	static void DeduplicateFilters(vector<unique_ptr<Expression>> &filters);
	void GenerateEquivalentFilters(const Expression &filter,
	                               const std::function<void(unique_ptr<Expression> filter)> &callback);
	//! The expression that a comparison, BETWEEN or IN filter compares with constants, or nullptr for other filters.
	//! Such a filter is false for NULL and has the same result for all values equal to that expression.
	static optional_ptr<const Expression> GetComparedExpression(const Expression &filter);
	//! Generates key constraints implied for the target side by equality conditions, e.g. [i = a, i = 500] => [a =
	//! 500]. Replaces only whole keys, including in NULL tests and Boolean combinations, preserving value equality.
	static void GenerateJoinFilters(const vector<JoinCondition> &conditions,
	                                const vector<reference<const Expression>> &filters, JoinSide target_side,
	                                const std::function<void(unique_ptr<Expression> filter)> &callback);
	//! Append generated filters to the scan's existing filters.
	void GenerateTableScanFilters(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                              vector<FilterPushdownResult> &pushdown_results);

	FilterPushdownResult TryPushdownGenericExpression(LogicalGet &get, Expression &expr);

private:
	void AddProjectionEqualities(const vector<unique_ptr<Expression>> &filters);
	void GenerateEquivalentKeyFilters(const Expression &filter,
	                                  const std::function<void(unique_ptr<Expression> filter)> &callback);
	FilterResult AddFilter(Expression &expr);
	FilterResult AddBoundComparisonFilter(Expression &expr);
	FilterResult AddTransitiveFilters(BoundFunctionExpression &comparison, bool is_root = true);
	unique_ptr<Expression> FindTransitiveFilter(const Expression &expr);
	Expression &GetNode(Expression &expr);
	idx_t GetEquivalenceSet(Expression &expr);

	FilterPushdownResult TryPushdownConstantFilter(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                               column_t column_id, vector<ExpressionValueInformation> &info_list);
	FilterPushdownResult TryPushdownExpression(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                           Expression &expr);
	FilterPushdownResult TryPushdownPrefixFilter(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                             Expression &expr);
	FilterPushdownResult TryPushdownLikeFilter(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                           Expression &expr);
	FilterPushdownResult TryPushdownInFilter(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                         Expression &expr);
	FilterPushdownResult TryPushdownOrClause(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                                         Expression &expr);
	FilterPushdownResult TryPushdownTemporalCastFilter(TableFilterSet &table_filters,
	                                                   const vector<ColumnIndex> &column_ids, Expression &expr);
	void TryPushdownRelaxedFilter(TableFilterSet &table_filters, const vector<ColumnIndex> &column_ids,
	                              vector<FilterPushdownResult> &pushdown_results, column_t expr_id,
	                              vector<ExpressionValueInformation> &info_list);

private:
	vector<unique_ptr<Expression>> remaining_filters;

	expression_map_t<unique_ptr<Expression>> stored_expressions;
	expression_map_t<idx_t> equivalence_set_map;
	map<idx_t, vector<ExpressionValueInformation>> constant_values;
	map<idx_t, vector<reference<Expression>>> equivalence_map;
	idx_t set_index = 0;
};

} // namespace duckdb
