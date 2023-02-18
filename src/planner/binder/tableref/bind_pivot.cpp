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

unique_ptr<SelectNode> Binder::BindPivot(PivotRef &ref, vector<unique_ptr<ParsedExpression>> all_columns) {
	const static idx_t PIVOT_EXPRESSION_LIMIT = 10000;
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
		if (!pivot.pivot_enum.empty()) {
			auto type = Catalog::GetType(context, INVALID_CATALOG, INVALID_SCHEMA, pivot.pivot_enum);
			if (type.id() != LogicalTypeId::ENUM) {
				throw BinderException(
				    FormatError(*aggr, StringUtil::Format("Pivot must reference an ENUM type: \"%s\" is of type \"%s\"",
				                                          pivot.pivot_enum, type.ToString())));
			}
			auto enum_size = EnumType::GetSize(type);
			for (idx_t i = 0; i < enum_size; i++) {
				pivot.values.emplace_back(EnumType::GetValue(Value::ENUM(i, type)));
			}
		}
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

	if (ref.groups.empty()) {
		// if rows are not specified any columns that are not pivoted/aggregated on are added to the GROUP BY clause
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
	} else {
		// if rows are specified only the columns mentioned in rows are added as groups
		for (auto &row : ref.groups) {
			select_node->groups.group_expressions.push_back(
			    make_unique<ConstantExpression>(Value::INTEGER(select_node->select_list.size() + 1)));
			select_node->select_list.push_back(make_unique<ColumnRefExpression>(row));
		}
	}
	// add the pivot expressions to the select list
	for (auto &pivot_expr : pivot_expressions) {
		select_node->select_list.push_back(std::move(pivot_expr));
	}
	return select_node;
}

unique_ptr<SelectNode> Binder::BindUnpivot(PivotRef &ref, vector<unique_ptr<ParsedExpression>> all_columns) {
	D_ASSERT(ref.groups.empty());
	D_ASSERT(ref.pivots.size() == 1);

	unique_ptr<ParsedExpression> expr;
	auto select_node = make_unique<SelectNode>();

	// handle the pivot
	auto &unpivot = ref.pivots[0];
	vector<Value> unpivot_names;
	vector<unique_ptr<ParsedExpression>> unpivot_expressions;

	case_insensitive_map_t<idx_t> handled_columns;
	for (idx_t i = 0; i < unpivot.values.size(); i++) {
		auto &col = unpivot.values[i];
		handled_columns[col.ToString()] = i;
	}

	unpivot_names.resize(handled_columns.size());
	unpivot_expressions.resize(handled_columns.size());
	for (auto &col_expr : all_columns) {
		if (col_expr->type != ExpressionType::COLUMN_REF) {
			throw InternalException("Unexpected child of pivot source - not a ColumnRef");
		}
		auto &columnref = (ColumnRefExpression &)*col_expr;
		auto column_name = columnref.GetColumnName();
		auto entry = handled_columns.find(column_name);
		if (entry == handled_columns.end()) {
			// not handled - add to the set of regularly selected columns
			select_node->select_list.push_back(std::move(col_expr));
		} else {
			auto idx = entry->second;
			unpivot_names[idx] = column_name;
			unpivot_expressions[idx] = make_unique<ColumnRefExpression>(column_name);
			handled_columns.erase(entry);
		}
	}
	if (!handled_columns.empty()) {
		for (auto &entry : handled_columns) {
			throw BinderException("Column \"%s\" referenced in UNPIVOT but no matching entry was found in the table",
			                      entry.first);
		}
	}
	// construct the UNNEST expression for the set of names (constant)
	auto unpivot_list = Value::LIST(LogicalType::VARCHAR, std::move(unpivot_names));
	auto unpivot_name_expr = make_unique<ConstantExpression>(std::move(unpivot_list));
	vector<unique_ptr<ParsedExpression>> unnest_name_children;
	unnest_name_children.push_back(std::move(unpivot_name_expr));
	auto unnest_name_expr = make_unique<FunctionExpression>("unnest", std::move(unnest_name_children));
	unnest_name_expr->alias = unpivot.name;
	select_node->select_list.push_back(std::move(unnest_name_expr));

	// construct the UNNEST expression for the set of unpivoted columns
	auto list_expr = make_unique<FunctionExpression>("list_value", std::move(unpivot_expressions));
	vector<unique_ptr<ParsedExpression>> unnest_val_children;
	unnest_val_children.push_back(std::move(list_expr));
	auto unnest_val_expr = make_unique<FunctionExpression>("unnest", std::move(unnest_val_children));
	unnest_val_expr->alias = ref.unpivot_name;
	select_node->select_list.push_back(std::move(unnest_val_expr));
	return select_node;
}

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

	unique_ptr<SelectNode> select_node;
	if (ref.aggregate) {
		select_node = BindPivot(ref, std::move(all_columns));
	} else {
		select_node = BindUnpivot(ref, std::move(all_columns));
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
