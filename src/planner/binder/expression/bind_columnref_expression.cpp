#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindStructExtract(ColumnRefExpression &colref, idx_t depth) {
	if (colref.table_name.empty()) {
		return BindResult("Empty table name");
	}
	// check if we can bind the table name as a column
	string actual_table_name;
	if (!binder.bind_context.TryGetMatchingBinding(colref.table_name, actual_table_name)) {
		return BindResult("Matching table not found");
	}
	// check if the column is a struct
	auto type = binder.bind_context.GetColumnType(actual_table_name, colref.table_name);
	if (type.id() != LogicalTypeId::STRUCT) {
		// not a struct!
		return BindResult("Not a struct");
	}
	// we can! try to bind this as a STRUCT_EXTRACT call instead
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ColumnRefExpression>(colref.table_name, actual_table_name));
	children.push_back(make_unique<ConstantExpression>(Value(colref.column_name)));
	auto function = make_unique_base<ParsedExpression, FunctionExpression>("struct_extract", children);
	function->alias = colref.GetName();
	return BindExpression(&function, depth);
}

BindResult ExpressionBinder::BindExpression(ColumnRefExpression &colref, idx_t depth) {
	D_ASSERT(!colref.column_name.empty());
	// individual column reference
	// resolve to either a base table or a subquery expression
	if (colref.table_name.empty()) {
		auto using_binding = binder.bind_context.GetUsingBinding(colref.column_name);
		if (using_binding) {
			// we are referencing a USING column
			// check if we can refer to one of the base columns directly
			unique_ptr<Expression> expression;
			if (!using_binding->primary_binding.empty()) {
				// we can! just assign the table name and re-bind
				colref.table_name = using_binding->primary_binding;
				return BindExpression(colref, depth);
			} else {
				// we cannot! we need to bind this as a coalesce between all the relevant columns
				auto coalesce = make_unique<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
				for (auto &entry : using_binding->bindings) {
					coalesce->children.push_back(make_unique<ColumnRefExpression>(colref.column_name, entry));
				}
				return BindExpression(*coalesce, depth);
			}
		}
		// no table name: find a binding that contains this
		if (binder.macro_binding != nullptr && binder.macro_binding->HasMatchingBinding(colref.column_name)) {
			// priority to macro parameter bindings TODO: throw a warning when this name conflicts
			colref.table_name = binder.macro_binding->alias;
		} else {
			colref.table_name = binder.bind_context.GetMatchingBinding(colref.column_name);
		}
		if (colref.table_name.empty()) {
			auto similar_bindings = binder.bind_context.GetSimilarBindings(colref.column_name);
			string candidate_str = StringUtil::CandidatesMessage(similar_bindings, "Candidate bindings");
			return BindResult(
			    binder.FormatError(colref, StringUtil::Format("Referenced column \"%s\" not found in FROM clause!%s",
			                                                  colref.column_name.c_str(), candidate_str)));
		}
	}
	// if it was a macro parameter, let macro_binding bind it to the argument
	BindResult result = binder.macro_binding != nullptr && colref.table_name == binder.macro_binding->alias
	                        ? binder.macro_binding->Bind(colref, depth)
	                        : binder.bind_context.BindColumn(colref, depth);
	if (!result.HasError()) {
		bound_columns = true;
	} else {
		// we failed to bind the column
		// check if we can bind the column as a struct extract call instead
		auto extract_result = BindStructExtract(colref, depth);
		if (!extract_result.HasError()) {
			return extract_result;
		}
		result.error = binder.FormatError(colref, result.error);
	}
	return result;
}

} // namespace duckdb
