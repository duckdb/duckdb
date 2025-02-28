#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_expanded_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

unique_ptr<Expression> CreateBoundStructExtract(ClientContext &context, unique_ptr<Expression> expr, string key) {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	arguments.push_back(make_uniq<BoundConstantExpression>(Value(key)));
	auto extract_function = GetKeyExtractFunction();
	auto bind_info = extract_function.bind(context, extract_function, arguments);
	auto return_type = extract_function.return_type;
	auto result = make_uniq<BoundFunctionExpression>(return_type, std::move(extract_function), std::move(arguments),
	                                                 std::move(bind_info));
	result->SetAlias(std::move(key));
	return std::move(result);
}

unique_ptr<Expression> CreateBoundStructExtractIndex(ClientContext &context, unique_ptr<Expression> expr, idx_t key) {
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(std::move(expr));
	arguments.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(int64_t(key))));
	auto extract_function = GetIndexExtractFunction();
	auto bind_info = extract_function.bind(context, extract_function, arguments);
	auto return_type = extract_function.return_type;
	auto result = make_uniq<BoundFunctionExpression>(return_type, std::move(extract_function), std::move(arguments),
	                                                 std::move(bind_info));
	result->SetAlias("element" + to_string(key));
	return std::move(result);
}

void SelectBinder::ThrowIfUnnestInLambda(const ColumnBinding &column_binding) {
	// Extract the unnests and check if any match the column index.
	for (auto &node_pair : node.unnests) {
		auto &unnest_node = node_pair.second;

		if (unnest_node.index == column_binding.table_index) {
			if (column_binding.column_index < unnest_node.expressions.size()) {
				throw BinderException("UNNEST in lambda expressions is not supported");
			}
		}
	}
}

BindResult SelectBinder::BindUnnest(FunctionExpression &function, idx_t depth, bool root_expression) {
	// bind the children of the function expression
	if (depth > 0) {
		return BindResult(BinderException(function, "UNNEST() for correlated expressions is not supported yet"));
	}

	ErrorData error;
	if (function.children.empty()) {
		return BindResult(BinderException(function, "UNNEST() requires a single argument"));
	}
	if (inside_window) {
		return BindResult(BinderException(function, UnsupportedUnnestMessage()));
	}

	if (function.distinct || function.filter || !function.order_bys->orders.empty()) {
		throw InvalidInputException("\"DISTINCT\", \"FILTER\", and \"ORDER BY\" are not "
		                            "applicable to \"UNNEST\"");
	}

	idx_t max_depth = 1;
	if (function.children.size() != 1) {
		bool has_parameter = false;
		bool supported_argument = false;
		for (idx_t i = 1; i < function.children.size(); i++) {
			if (has_parameter) {
				return BindResult(BinderException(function, "UNNEST() only supports a single additional argument"));
			}
			if (function.children[i]->HasParameter()) {
				throw ParameterNotAllowedException("Parameter not allowed in unnest parameter");
			}
			if (!function.children[i]->IsScalar()) {
				break;
			}
			auto alias = StringUtil::Lower(function.children[i]->GetAlias());
			BindChild(function.children[i], depth, error);
			if (error.HasError()) {
				return BindResult(std::move(error));
			}
			auto &const_child = BoundExpression::GetExpression(*function.children[i]);
			auto value = ExpressionExecutor::EvaluateScalar(context, *const_child, true);
			if (alias == "recursive") {
				auto recursive = value.GetValue<bool>();
				if (recursive) {
					max_depth = NumericLimits<idx_t>::Maximum();
				}
			} else if (alias == "max_depth") {
				max_depth = value.GetValue<uint32_t>();
				if (max_depth == 0) {
					throw BinderException("UNNEST cannot have a max depth of 0");
				}
			} else if (!alias.empty()) {
				throw BinderException("Unsupported parameter \"%s\" for unnest", alias);
			} else {
				break;
			}
			has_parameter = true;
			supported_argument = true;
		}
		if (!supported_argument) {
			return BindResult(BinderException(function, "UNNEST - unsupported extra argument, unnest only supports "
			                                            "recursive := [true/false] or max_depth := #"));
		}
	}
	unnest_level++;
	BindChild(function.children[0], depth, error);
	if (error.HasError()) {
		// failed to bind
		// try to bind correlated columns manually
		auto result = BindCorrelatedColumns(function.children[0], error);
		if (result.HasError()) {
			return BindResult(result.error);
		}
		auto &bound_expr = BoundExpression::GetExpression(*function.children[0]);
		ExtractCorrelatedExpressions(binder, *bound_expr);
	}
	auto &child = BoundExpression::GetExpression(*function.children[0]);
	auto &child_type = child->return_type;
	unnest_level--;

	if (unnest_level > 0) {
		throw BinderException(
		    function,
		    "Nested UNNEST calls are not supported - use UNNEST(x, recursive := true) to unnest multiple levels");
	}

	switch (child_type.id()) {
	case LogicalTypeId::UNKNOWN:
		throw ParameterNotResolvedException();
	case LogicalTypeId::LIST:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::SQLNULL:
		break;
	default:
		return BindResult(BinderException(function, "UNNEST() can only be applied to lists, structs and NULL, not %s",
		                                  child_type.ToString()));
	}

	idx_t list_unnests;
	idx_t struct_unnests = 0;

	auto unnest_expr = std::move(child);
	if (child_type.id() == LogicalTypeId::SQLNULL) {
		list_unnests = 1;
	} else {
		// perform all LIST unnests
		auto type = child_type;
		list_unnests = 0;
		while (type.id() == LogicalTypeId::LIST) {
			type = ListType::GetChildType(type);
			list_unnests++;
			if (list_unnests >= max_depth) {
				break;
			}
		}
		// unnest structs
		if (type.id() == LogicalTypeId::STRUCT) {
			struct_unnests = max_depth - list_unnests;
		}
	}
	if (struct_unnests > 0 && !root_expression) {
		child = std::move(unnest_expr);
		return BindResult(BinderException(
		    function, "UNNEST() on a struct column can only be applied as the root element of a SELECT expression"));
	}
	// perform all LIST unnests
	auto return_type = child_type;
	for (idx_t current_depth = 0; current_depth < list_unnests; current_depth++) {
		if (return_type.id() == LogicalTypeId::LIST) {
			return_type = ListType::GetChildType(return_type);
		}
		auto result = make_uniq<BoundUnnestExpression>(return_type);
		result->child = std::move(unnest_expr);
		auto alias = function.GetAlias().empty() ? result->ToString() : function.GetAlias();

		auto current_level = unnest_level + list_unnests - current_depth - 1;
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
		unnest_expr = make_uniq<BoundColumnRefExpression>(
		    std::move(alias), return_type, ColumnBinding(unnest_table_index, unnest_column_index), depth);
	}
	// now perform struct unnests, if any
	if (struct_unnests > 0) {
		vector<unique_ptr<Expression>> struct_expressions;
		struct_expressions.push_back(std::move(unnest_expr));

		for (idx_t i = 0; i < struct_unnests; i++) {
			vector<unique_ptr<Expression>> new_expressions;
			// check if there are any structs left
			bool has_structs = false;
			for (auto &expr : struct_expressions) {
				if (expr->return_type.id() == LogicalTypeId::STRUCT) {
					// struct! push a struct_extract
					auto &child_types = StructType::GetChildTypes(expr->return_type);
					if (StructType::IsUnnamed(expr->return_type)) {
						for (idx_t child_index = 0; child_index < child_types.size(); child_index++) {
							new_expressions.push_back(
							    CreateBoundStructExtractIndex(context, expr->Copy(), child_index + 1));
						}
					} else {
						for (auto &entry : child_types) {
							new_expressions.push_back(CreateBoundStructExtract(context, expr->Copy(), entry.first));
						}
					}
					has_structs = true;
				} else {
					// not a struct - push as-is
					new_expressions.push_back(std::move(expr));
				}
			}
			struct_expressions = std::move(new_expressions);
			if (!has_structs) {
				break;
			}
		}
		unnest_expr = make_uniq<BoundExpandedExpression>(std::move(struct_expressions));
	}
	return BindResult(std::move(unnest_expr));
}

} // namespace duckdb
