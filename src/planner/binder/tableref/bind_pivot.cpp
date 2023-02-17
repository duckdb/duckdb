#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(PivotRef &ref) {
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

	// now handle the actual pivot
	// keep track of the columns by which we pivot/aggregate
	// any columns which are not pivoted/aggregated on are added to the GROUP BY clause
	auto select_node = make_unique<SelectNode>();
	case_insensitive_set_t handled_columns;
	case_insensitive_set_t pivots;
	vector<unique_ptr<ParsedExpression>> pivot_expressions;
	for (auto &pivot : ref.pivots) {
		// add the pivoted column to the columns that have been handled
		handled_columns.insert(pivot.name);
		for (auto &val : pivot.values) {
			for (auto &aggr : ref.aggregates) {
				if (aggr->type != ExpressionType::FUNCTION) {
					throw BinderException(FormatError(*aggr, "Pivot expression must be an aggregate"));
				}
				auto copy = aggr->Copy();
				auto &function = (FunctionExpression &)*copy;
				if (function.children.size() != 1) {
					throw BinderException(FormatError(*aggr, "Pivot expression must have a single argument"));
				}
				if (function.children[0]->type != ExpressionType::COLUMN_REF) {
					throw BinderException(
					    FormatError(*aggr, "Pivot expression must have a single column reference as argument"));
				}
				auto &child_colref = (ColumnRefExpression &)*function.children[0];
				handled_columns.insert(child_colref.GetColumnName());

				auto column_ref = make_unique<ColumnRefExpression>(pivot.name);
				auto constant_value = make_unique<ConstantExpression>(val);
				auto comp_expr = make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                   std::move(column_ref), std::move(constant_value));
				function.filter = std::move(comp_expr);
				function.alias = val.ToString();
				if (pivots.find(function.alias) != pivots.end()) {
					throw BinderException(FormatError(
					    *aggr, StringUtil::Format("The column \"%s\" was specified multiple times", function.alias)));
				}
				pivots.insert(function.alias);
				pivot_expressions.push_back(std::move(copy));
			}
		}
	}
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
