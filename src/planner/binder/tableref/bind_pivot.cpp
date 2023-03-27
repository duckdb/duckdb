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
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

namespace duckdb {

static void ConstructPivots(PivotRef &ref, idx_t pivot_idx, vector<unique_ptr<ParsedExpression>> &pivot_expressions,
                            unique_ptr<ParsedExpression> current_expr = nullptr,
                            const string &current_name = string()) {
	auto &pivot = ref.pivots[pivot_idx];
	bool last_pivot = pivot_idx + 1 == ref.pivots.size();
	for (auto &entry : pivot.entries) {
		unique_ptr<ParsedExpression> expr = current_expr ? current_expr->Copy() : nullptr;
		string name = entry.alias;
		D_ASSERT(entry.values.size() == pivot.pivot_expressions.size());
		for (idx_t v = 0; v < entry.values.size(); v++) {
			auto &value = entry.values[v];
			auto column_ref = pivot.pivot_expressions[v]->Copy();
			auto constant_value = make_unique<ConstantExpression>(value);
			auto comp_expr = make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
			                                                   std::move(column_ref), std::move(constant_value));
			if (expr) {
				expr = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
				                                          std::move(comp_expr));
			} else {
				expr = std::move(comp_expr);
			}
			if (entry.alias.empty()) {
				if (name.empty()) {
					name = value.ToString();
				} else {
					name += "_" + value.ToString();
				}
			}
		}
		if (!current_name.empty()) {
			name = current_name + "_" + name;
		}
		if (last_pivot) {
			// construct the aggregates
			for (auto &aggr : ref.aggregates) {
				auto copy = aggr->Copy();
				auto &function = (FunctionExpression &)*copy;
				// add the filter and alias to the aggregate function
				function.filter = expr->Copy();
				if (ref.aggregates.size() > 1 || !function.alias.empty()) {
					// if there are multiple aggregates specified we add the name of the aggregate as well
					function.alias = name + "_" + function.GetName();
				} else {
					function.alias = name;
				}
				pivot_expressions.push_back(std::move(copy));
			}
		} else {
			// need to recurse
			ConstructPivots(ref, pivot_idx + 1, pivot_expressions, std::move(expr), name);
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
	for (auto &aggr : ref.aggregates) {
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
	}
	value_set_t pivots;

	// now handle the pivots
	auto select_node = make_unique<SelectNode>();
	// first add all pivots to the set of handled columns, and check for duplicates
	idx_t total_pivots = 1;
	for (auto &pivot : ref.pivots) {
		if (!pivot.pivot_enum.empty()) {
			auto type = Catalog::GetType(context, INVALID_CATALOG, INVALID_SCHEMA, pivot.pivot_enum);
			if (type.id() != LogicalTypeId::ENUM) {
				throw BinderException(
				    FormatError(ref, StringUtil::Format("Pivot must reference an ENUM type: \"%s\" is of type \"%s\"",
				                                        pivot.pivot_enum, type.ToString())));
			}
			auto enum_size = EnumType::GetSize(type);
			for (idx_t i = 0; i < enum_size; i++) {
				auto enum_value = EnumType::GetValue(Value::ENUM(i, type));
				PivotColumnEntry entry;
				entry.values.emplace_back(enum_value);
				entry.alias = std::move(enum_value);
				pivot.entries.push_back(std::move(entry));
			}
		}
		total_pivots *= pivot.entries.size();
		// add the pivoted column to the columns that have been handled
		for (auto &pivot_name : pivot.pivot_expressions) {
			ExtractPivotExpressions(*pivot_name, handled_columns);
		}
		value_set_t pivots;
		for (auto &entry : pivot.entries) {
			D_ASSERT(!entry.star_expr);
			Value val;
			if (entry.values.size() == 1) {
				val = entry.values[0];
			} else {
				val = Value::LIST(LogicalType::VARCHAR, entry.values);
			}
			if (pivots.find(val) != pivots.end()) {
				throw BinderException(FormatError(
				    ref, StringUtil::Format("The value \"%s\" was specified multiple times in the IN clause",
				                            val.ToString())));
			}
			if (entry.values.size() != pivot.pivot_expressions.size()) {
				throw ParserException("PIVOT IN list - inconsistent amount of rows - expected %d but got %d",
				                      pivot.pivot_expressions.size(), entry.values.size());
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

unique_ptr<SelectNode> Binder::BindUnpivot(Binder &child_binder, PivotRef &ref,
                                           vector<unique_ptr<ParsedExpression>> all_columns,
                                           unique_ptr<ParsedExpression> &where_clause) {
	D_ASSERT(ref.groups.empty());
	D_ASSERT(ref.pivots.size() == 1);

	unique_ptr<ParsedExpression> expr;
	auto select_node = make_unique<SelectNode>();

	// handle the pivot
	auto &unpivot = ref.pivots[0];

	// handle star expressions in any entries
	vector<PivotColumnEntry> new_entries;
	for (auto &entry : unpivot.entries) {
		if (entry.star_expr) {
			D_ASSERT(entry.values.empty());
			vector<unique_ptr<ParsedExpression>> star_columns;
			child_binder.ExpandStarExpression(std::move(entry.star_expr), star_columns);

			for (auto &col : star_columns) {
				if (col->type != ExpressionType::COLUMN_REF) {
					throw InternalException("Unexpected child of unpivot star - not a ColumnRef");
				}
				auto &columnref = (ColumnRefExpression &)*col;
				PivotColumnEntry new_entry;
				new_entry.values.emplace_back(columnref.GetColumnName());
				new_entry.alias = columnref.GetColumnName();
				new_entries.push_back(std::move(new_entry));
			}
		} else {
			new_entries.push_back(std::move(entry));
		}
	}
	unpivot.entries = std::move(new_entries);

	case_insensitive_set_t handled_columns;
	case_insensitive_map_t<string> name_map;
	for (auto &entry : unpivot.entries) {
		for (auto &value : entry.values) {
			handled_columns.insert(value.ToString());
		}
	}

	for (auto &col_expr : all_columns) {
		if (col_expr->type != ExpressionType::COLUMN_REF) {
			throw InternalException("Unexpected child of pivot source - not a ColumnRef");
		}
		auto &columnref = (ColumnRefExpression &)*col_expr;
		auto &column_name = columnref.GetColumnName();
		auto entry = handled_columns.find(column_name);
		if (entry == handled_columns.end()) {
			// not handled - add to the set of regularly selected columns
			select_node->select_list.push_back(std::move(col_expr));
		} else {
			name_map[column_name] = column_name;
			handled_columns.erase(entry);
		}
	}
	if (!handled_columns.empty()) {
		for (auto &entry : handled_columns) {
			throw BinderException("Column \"%s\" referenced in UNPIVOT but no matching entry was found in the table",
			                      entry);
		}
	}
	vector<Value> unpivot_names;
	for (auto &entry : unpivot.entries) {
		string generated_name;
		for (auto &val : entry.values) {
			auto name_entry = name_map.find(val.ToString());
			if (name_entry == name_map.end()) {
				throw InternalException("Unpivot - could not find column name in name map");
			}
			if (!generated_name.empty()) {
				generated_name += "_";
			}
			generated_name += name_entry->second;
		}
		unpivot_names.emplace_back(!entry.alias.empty() ? entry.alias : generated_name);
	}
	vector<vector<unique_ptr<ParsedExpression>>> unpivot_expressions;
	for (idx_t v_idx = 0; v_idx < unpivot.entries[0].values.size(); v_idx++) {
		vector<unique_ptr<ParsedExpression>> expressions;
		expressions.reserve(unpivot.entries.size());
		for (auto &entry : unpivot.entries) {
			expressions.push_back(make_unique<ColumnRefExpression>(entry.values[v_idx].ToString()));
		}
		unpivot_expressions.push_back(std::move(expressions));
	}

	// construct the UNNEST expression for the set of names (constant)
	auto unpivot_list = Value::LIST(LogicalType::VARCHAR, std::move(unpivot_names));
	auto unpivot_name_expr = make_unique<ConstantExpression>(std::move(unpivot_list));
	vector<unique_ptr<ParsedExpression>> unnest_name_children;
	unnest_name_children.push_back(std::move(unpivot_name_expr));
	auto unnest_name_expr = make_unique<FunctionExpression>("unnest", std::move(unnest_name_children));
	unnest_name_expr->alias = unpivot.unpivot_names[0];
	select_node->select_list.push_back(std::move(unnest_name_expr));

	// construct the UNNEST expression for the set of unpivoted columns
	if (ref.unpivot_names.size() != unpivot_expressions.size()) {
		throw BinderException("UNPIVOT name count mismatch - got %d names but %d expressions", ref.unpivot_names.size(),
		                      unpivot_expressions.size());
	}
	for (idx_t i = 0; i < unpivot_expressions.size(); i++) {
		auto list_expr = make_unique<FunctionExpression>("list_value", std::move(unpivot_expressions[i]));
		vector<unique_ptr<ParsedExpression>> unnest_val_children;
		unnest_val_children.push_back(std::move(list_expr));
		auto unnest_val_expr = make_unique<FunctionExpression>("unnest", std::move(unnest_val_children));
		auto unnest_name = i < ref.column_name_alias.size() ? ref.column_name_alias[i] : ref.unpivot_names[i];
		unnest_val_expr->alias = unnest_name;
		select_node->select_list.push_back(std::move(unnest_val_expr));
		if (!ref.include_nulls) {
			// if we are running with EXCLUDE NULLS we need to add an IS NOT NULL filter
			auto colref = make_unique<ColumnRefExpression>(unnest_name);
			auto filter = make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(colref));
			if (where_clause) {
				where_clause = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                  std::move(where_clause), std::move(filter));
			} else {
				where_clause = std::move(filter);
			}
		}
	}
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
	unique_ptr<ParsedExpression> where_clause;
	if (!ref.aggregates.empty()) {
		select_node = BindPivot(ref, std::move(all_columns));
	} else {
		select_node = BindUnpivot(*child_binder, ref, std::move(all_columns), where_clause);
	}
	// bind the generated select node
	auto bound_select_node = child_binder->BindSelectNode(*select_node, std::move(from_table));
	auto root_index = bound_select_node->GetRootIndex();
	BoundQueryNode *bound_select_ptr = bound_select_node.get();

	unique_ptr<BoundTableRef> result;
	MoveCorrelatedExpressions(*child_binder);
	result = make_unique<BoundSubqueryRef>(std::move(child_binder), std::move(bound_select_node));
	auto alias = ref.alias.empty() ? "__unnamed_pivot" : ref.alias;
	SubqueryRef subquery_ref(nullptr, alias);
	subquery_ref.column_name_alias = std::move(ref.column_name_alias);
	if (where_clause) {
		// if a WHERE clause was provided - bind a subquery holding the WHERE clause
		// we need to bind a new subquery here because the WHERE clause has to be applied AFTER the unnest
		child_binder = Binder::CreateBinder(context, this);
		child_binder->bind_context.AddSubquery(root_index, subquery_ref.alias, subquery_ref, *bound_select_ptr);
		auto where_query = make_unique<SelectNode>();
		where_query->select_list.push_back(make_unique<StarExpression>());
		where_query->where_clause = std::move(where_clause);
		bound_select_node = child_binder->BindSelectNode(*where_query, std::move(result));
		bound_select_ptr = bound_select_node.get();
		root_index = bound_select_node->GetRootIndex();
		result = make_unique<BoundSubqueryRef>(std::move(child_binder), std::move(bound_select_node));
	}
	bind_context.AddSubquery(root_index, subquery_ref.alias, subquery_ref, *bound_select_ptr);
	return result;
}

} // namespace duckdb
