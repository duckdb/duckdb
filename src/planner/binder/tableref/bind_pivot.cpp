#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

static void ConstructPivots(PivotRef &ref, idx_t pivot_idx, vector<unique_ptr<ParsedExpression>> &pivot_expressions,
                            unique_ptr<ParsedExpression> current_expr = nullptr, string current_name = string()) {
	auto &pivot = ref.pivots[pivot_idx];
	bool last_pivot = pivot_idx + 1 == ref.pivots.size();
	for (auto &value : pivot.values) {
		auto column_ref = make_unique<ColumnRefExpression>(pivot.name);
		auto constant_value = make_unique<ConstantExpression>(value);
		auto comp_expr = make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
		                                                   std::move(column_ref), std::move(constant_value));

		unique_ptr<ParsedExpression> expr;
		string name;
		if (current_expr) {
			expr = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, current_expr->Copy(),
			                                          std::move(comp_expr));
		} else {
			expr = std::move(comp_expr);
		}
		if (!current_name.empty()) {
			name = current_name + " " + value.ToString();
		} else {
			name = value.ToString();
		}
		if (last_pivot) {
			// construct the aggregate
			auto copy = ref.aggregate->Copy();
			auto &function = (FunctionExpression &)*copy;
			// add the filter and alias to the aggregate function
			function.filter = std::move(expr);
			function.alias = name;
			pivot_expressions.push_back(std::move(copy));
		} else {
			// need to recurse
			ConstructPivots(ref, pivot_idx + 1, pivot_expressions, std::move(expr), std::move(name));
		}
	}
}

static void ExtractPivotExpressions(ParsedExpression &expr, case_insensitive_set_t &handled_columns) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &child_colref = (ColumnRefExpression &)expr;
		if (child_colref.IsQualified()) {
			throw BinderException("PIVOT expression cannot contain qualified columns");
		}
		handled_columns.insert(child_colref.GetColumnName());
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { ExtractPivotExpressions(child, handled_columns); });
}

unique_ptr<BoundTableRef> Binder::Bind(PivotRef &ref) {
	const static idx_t PIVOT_EXPRESSION_LIMIT = 10000;
	if (!ref.source) {
		throw InternalException("Pivot without a source!?");
	}

	// bind the source of the pivot
	auto child_binder = Binder::CreateBinder(context, this);
	auto from_table = child_binder->Bind(*ref.source);

	// figure out the set of column names that are in the source of the pivot
	vector<unique_ptr<ParsedExpression>> all_columns;
	child_binder->ExpandStarExpression(make_unique<StarExpression>(), all_columns);

	vector<string> names;
	for (auto &entry : all_columns) {
		if (entry->type != ExpressionType::COLUMN_REF) {
			throw InternalException("Unexpected child of pivot source - not a ColumnRef");
		}
		auto &columnref = (ColumnRefExpression &)*entry;
		names.push_back(columnref.GetColumnName());
	}

	// keep track of the columns by which we pivot/aggregate
	// any columns which are not pivoted/aggregated on are added to the GROUP BY clause
	case_insensitive_set_t handled_columns;
	// parse the aggregate, and extract the referenced columns from the aggregate
	auto &aggr = ref.aggregate;
	if (aggr->type != ExpressionType::FUNCTION) {
		throw BinderException(FormatError(*aggr, "Pivot expression must be an aggregate"));
	}
	if (aggr->HasSubquery()) {
		throw BinderException(FormatError(*aggr, "Pivot expression cannot contain subqueries"));
	}
	if (aggr->IsWindow()) {
		throw BinderException(FormatError(*aggr, "Pivot expression cannot contain window functions"));
	}
	ExtractPivotExpressions(*aggr, handled_columns);

	// now handle the pivots
	auto select_node = make_unique<SelectNode>();
	// first add all pivots to the set of handled columns, and check for duplicates
	idx_t total_pivots = 1;
	for (auto &pivot : ref.pivots) {
		total_pivots *= pivot.values.size();
		// add the pivoted column to the columns that have been handled
		handled_columns.insert(pivot.name);
		value_set_t pivots;
		for (auto &val : pivot.values) {
			if (pivots.find(val) != pivots.end()) {
				throw BinderException(FormatError(
				    *aggr, StringUtil::Format("The value \"%s\" was specified multiple times in the IN clause",
				                              val.ToString())));
			}
			pivots.insert(val);
		}
	}
	if (total_pivots >= PIVOT_EXPRESSION_LIMIT) {
		throw BinderException("Pivot column limit of %llu exceeded", PIVOT_EXPRESSION_LIMIT);
	}
	// now construct the actual aggregates
	// note that we construct a cross-product of all pivots
	// we do this recursively
	vector<unique_ptr<ParsedExpression>> pivot_expressions;
	ConstructPivots(ref, 0, pivot_expressions);

	// any columns that are not pivoted/aggregated on are added to the GROUP BY clause
	for (auto &entry : all_columns) {
		if (entry->type != ExpressionType::COLUMN_REF) {
			throw InternalException("Unexpected child of pivot source - not a ColumnRef");
		}
		auto &columnref = (ColumnRefExpression &)*entry;
		if (handled_columns.find(columnref.GetColumnName()) == handled_columns.end()) {
			// not handled - add to grouping set
			select_node->groups.group_expressions.push_back(
			    make_unique<ConstantExpression>(Value::INTEGER(select_node->select_list.size() + 1)));
			select_node->select_list.push_back(std::move(entry));
		}
	}
	// add the pivot expressions to the select list
	for (auto &pivot_expr : pivot_expressions) {
		select_node->select_list.push_back(std::move(pivot_expr));
	}
	// bind the generated select node
	auto bound_select_node = child_binder->BindSelectNode(*select_node, std::move(from_table));
	auto alias = ref.alias.empty() ? "__unnamed_pivot" : ref.alias;
	SubqueryRef subquery_ref(nullptr, alias);
	subquery_ref.column_name_alias = std::move(ref.column_name_alias);
	bind_context.AddSubquery(bound_select_node->GetRootIndex(), subquery_ref.alias, subquery_ref, *bound_select_node);
	auto result = make_unique<BoundSubqueryRef>(std::move(child_binder), std::move(bound_select_node));
	return std::move(result);
}

} // namespace duckdb
