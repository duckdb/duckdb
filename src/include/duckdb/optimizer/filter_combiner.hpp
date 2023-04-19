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
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include "duckdb/storage/data_table.hpp"
#include <functional>
#include <map>

namespace duckdb {
class Optimizer;

enum class ValueComparisonResult { PRUNE_LEFT, PRUNE_RIGHT, UNSATISFIABLE_CONDITION, PRUNE_NOTHING };
enum class FilterResult { UNSATISFIABLE, SUCCESS, UNSUPPORTED };

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
	struct ExpressionValueInformation {
		Value constant;
		ExpressionType comparison_type;
	};

	FilterResult AddFilter(unique_ptr<Expression> expr);

	void GenerateFilters(const std::function<void(unique_ptr<Expression> filter)> &callback);
	bool HasFilters();
	TableFilterSet GenerateTableScanFilters(vector<idx_t> &column_ids);
	// vector<unique_ptr<TableFilter>> GenerateZonemapChecks(vector<idx_t> &column_ids, vector<unique_ptr<TableFilter>>
	// &pushed_filters);

private:
	FilterResult AddFilter(Expression &expr);
	FilterResult AddBoundComparisonFilter(Expression &expr);
	FilterResult AddTransitiveFilters(BoundComparisonExpression &comparison);
	unique_ptr<Expression> FindTransitiveFilter(Expression &expr);
	// unordered_map<idx_t, std::pair<Value *, Value *>>
	// FindZonemapChecks(vector<idx_t> &column_ids, unordered_set<idx_t> &not_constants, Expression *filter);
	Expression &GetNode(Expression &expr);
	idx_t GetEquivalenceSet(Expression &expr);
	FilterResult AddConstantComparison(vector<ExpressionValueInformation> &info_list, ExpressionValueInformation info);
	//
	//	//! Functions used to push and generate OR Filters
	//	void LookUpConjunctions(Expression *expr);
	//	bool BFSLookUpConjunctions(BoundConjunctionExpression *conjunction);
	//	void VerifyOrsToPush(Expression &expr);
	//
	//	bool UpdateConjunctionFilter(BoundComparisonExpression *comparison_expr);
	//	bool UpdateFilterByColumn(BoundColumnRefExpression *column_ref, BoundComparisonExpression *comparison_expr);
	//	void GenerateORFilters(TableFilterSet &table_filter, vector<idx_t> &column_ids);
	//
	//	template <typename CONJUNCTION_TYPE>
	//	void GenerateConjunctionFilter(BoundConjunctionExpression *conjunction, ConjunctionFilter *last_conj_filter) {
	//		auto new_filter = NextConjunctionFilter<CONJUNCTION_TYPE>(conjunction);
	//		auto conj_filter_ptr = (ConjunctionFilter *)new_filter.get();
	//		last_conj_filter->child_filters.push_back(std::move(new_filter));
	//		last_conj_filter = conj_filter_ptr;
	//	}
	//
	//	template <typename CONJUNCTION_TYPE>
	//	unique_ptr<TableFilter> NextConjunctionFilter(BoundConjunctionExpression *conjunction) {
	//		unique_ptr<ConjunctionFilter> conj_filter = make_uniq<CONJUNCTION_TYPE>();
	//		for (auto &expr : conjunction->children) {
	//			auto comp_expr = (BoundComparisonExpression *)expr.get();
	//			auto &const_expr =
	//			    (comp_expr->left->type == ExpressionType::VALUE_CONSTANT) ? *comp_expr->left : *comp_expr->right;
	//			auto const_value = ExpressionExecutor::EvaluateScalar(const_expr);
	//			auto const_filter = make_uniq<ConstantFilter>(comp_expr->type, const_value);
	//			conj_filter->child_filters.push_back(std::move(const_filter));
	//		}
	//		return std::move(conj_filter);
	//	}

private:
	vector<unique_ptr<Expression>> remaining_filters;

	expression_map_t<unique_ptr<Expression>> stored_expressions;
	expression_map_t<idx_t> equivalence_set_map;
	unordered_map<idx_t, vector<ExpressionValueInformation>> constant_values;
	unordered_map<idx_t, vector<reference<Expression>>> equivalence_map;
	idx_t set_index = 0;
	//
	//	//! Structures used for OR Filters
	//
	//	struct ConjunctionsToPush {
	//		BoundConjunctionExpression *root_or;
	//
	//		// only preserve AND if there is a single column in the expression
	//		bool preserve_and = true;
	//
	//		// conjunction chain for this column
	//		vector<unique_ptr<BoundConjunctionExpression>> conjunctions;
	//	};
	//
	//	expression_map_t<vector<unique_ptr<ConjunctionsToPush>>> map_col_conjunctions;
	//	vector<BoundColumnRefExpression *> vec_colref_insertion_order;
	//
	//	BoundConjunctionExpression *cur_root_or;
	//	BoundConjunctionExpression *cur_conjunction;
	//
	//	BoundColumnRefExpression *cur_colref_to_push;
};

} // namespace duckdb
