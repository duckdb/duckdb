#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult SelectBinder::BindUnnest(FunctionExpression &function, idx_t depth) {
	// bind the children of the function expression
	string error;
	if (function.children.empty()) {
		return BindResult(binder.FormatError(function, "UNNEST() requires a single argument"));
	}
	bool recursive = false;
	if (function.children.size() != 1) {
		bool supported_argument = false;
		for (idx_t i = 1; i < function.children.size(); i++) {
			if (function.children[i]->HasParameter()) {
				throw ParameterNotAllowedException("Parameter not allowed in unnest parameter");
			}
			if (!function.children[i]->IsScalar()) {
				break;
			}
			if (function.children[i]->alias != "recursive") {
				break;
			}
			BindChild(function.children[i], depth, error);
			if (!error.empty()) {
				return BindResult(error);
			}
			auto &const_child = (BoundExpression &)*function.children[i];
			recursive = ExpressionExecutor::EvaluateScalar(context, *const_child.expr, true).GetValue<bool>();
			supported_argument = true;
		}
		if (!supported_argument) {
			return BindResult(binder.FormatError(
			    function, "UNNEST - unsupported extra argument, unnest only supports recursive := [true/false]"));
		}
	}
	unnest_level++;
	BindChild(function.children[0], depth, error);
	if (!error.empty()) {
		// failed to bind
		// try to bind correlated columns manually
		if (!BindCorrelatedColumns(function.children[0])) {
			return BindResult(error);
		}
		auto bound_expr = (BoundExpression *)function.children[0].get();
		ExtractCorrelatedExpressions(binder, *bound_expr->expr);
	}
	auto &child = (BoundExpression &)*function.children[0];
	auto &child_type = child.expr->return_type;

	unnest_level--;
	if (child_type.id() != LogicalTypeId::LIST && child_type.id() != LogicalTypeId::SQLNULL &&
	    child_type.id() != LogicalTypeId::UNKNOWN) {
		return BindResult(binder.FormatError(function, "UNNEST() can only be applied to lists and NULL"));
	}

	if (depth > 0) {
		throw BinderException(binder.FormatError(function, "UNNEST() for correlated expressions is not supported yet"));
	}
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	auto return_type = child_type;

	auto unnest_expr = std::move(child.expr);
	idx_t total_unnests;
	if (recursive && child_type.id() == LogicalTypeId::LIST) {
		// figure out how many times to unnest if this is a recursive unnest
		auto type = child_type;
		total_unnests = 0;
		while (type.id() == LogicalTypeId::LIST) {
			type = ListType::GetChildType(type);
			total_unnests++;
		}
	} else {
		total_unnests = 1;
	}
	for (idx_t current_depth = 0; current_depth < total_unnests; current_depth++) {
		if (return_type.id() == LogicalTypeId::LIST) {
			return_type = ListType::GetChildType(return_type);
		}
		auto result = make_unique<BoundUnnestExpression>(return_type);
		result->child = std::move(unnest_expr);
		auto alias = function.alias.empty() ? result->ToString() : function.alias;

		auto current_level = unnest_level + total_unnests - current_depth - 1;
		auto entry = node.unnests.find(current_level);
		idx_t unnest_table_index;
		idx_t unnest_column_index;
		if (entry == node.unnests.end()) {
			BoundUnnestNode unnest_node;
			unnest_node.index = binder.GenerateTableIndex();
			unnest_node.expressions.push_back(std::move(result));
			unnest_table_index = unnest_node.index;
			unnest_column_index = 0;
			node.unnests.insert(make_pair(current_level, std::move(unnest_node)));
		} else {
			unnest_table_index = entry->second.index;
			unnest_column_index = entry->second.expressions.size();
			entry->second.expressions.push_back(std::move(result));
		}
		// now create a column reference referring to the unnest
		unnest_expr = make_unique<BoundColumnRefExpression>(
		    std::move(alias), return_type, ColumnBinding(unnest_table_index, unnest_column_index), depth);
	}
	return BindResult(std::move(unnest_expr));
}

} // namespace duckdb
