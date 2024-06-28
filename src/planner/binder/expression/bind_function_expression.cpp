#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::TryBindLambdaOrJson(FunctionExpression &function, idx_t depth, CatalogEntry &func) {

	auto lambda_bind_result = BindLambdaFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	if (!lambda_bind_result.HasError()) {
		return lambda_bind_result;
	}

	auto json_bind_result = BindFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	if (!json_bind_result.HasError()) {
		return json_bind_result;
	}

	return BindResult("failed to bind function, either: " + lambda_bind_result.error.RawMessage() +
	                  "\n"
	                  " or: " +
	                  json_bind_result.error.RawMessage());
}

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth,
                                            unique_ptr<ParsedExpression> &expr_ptr) {
	// lookup the function in the catalog
	QueryErrorContext error_context(function.query_location);
	binder.BindSchemaOrCatalog(function.catalog, function.schema);
	auto func = GetCatalogEntry(CatalogType::SCALAR_FUNCTION_ENTRY, function.catalog, function.schema,
	                            function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
	if (!func) {
		// function was not found - check if we this is a table function
		auto table_func = GetCatalogEntry(CatalogType::TABLE_FUNCTION_ENTRY, function.catalog, function.schema,
		                                  function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
		if (table_func) {
			throw BinderException(function,
			                      "Function \"%s\" is a table function but it was used as a scalar function. This "
			                      "function has to be called in a FROM clause (similar to a table).",
			                      function.function_name);
		}
		// not a table function - check if the schema is set
		if (!function.schema.empty()) {
			// the schema is set - check if we can turn this the schema into a column ref
			ErrorData error;
			unique_ptr<ColumnRefExpression> colref;
			if (function.catalog.empty()) {
				colref = make_uniq<ColumnRefExpression>(function.schema);
			} else {
				colref = make_uniq<ColumnRefExpression>(function.schema, function.catalog);
			}
			auto new_colref = QualifyColumnName(*colref, error);
			bool is_col = !error.HasError();
			bool is_col_alias = QualifyColumnAlias(*colref);

			if (is_col || is_col_alias) {
				// we can! transform this into a function call on the column
				// i.e. "x.lower()" becomes "lower(x)"
				function.children.insert(function.children.begin(), std::move(colref));
				function.catalog = INVALID_CATALOG;
				function.schema = INVALID_SCHEMA;
			}
		}
		// rebind the function
		func = GetCatalogEntry(CatalogType::SCALAR_FUNCTION_ENTRY, function.catalog, function.schema,
		                       function.function_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	}

	if (func->type != CatalogType::AGGREGATE_FUNCTION_ENTRY &&
	    (function.distinct || function.filter || !function.order_bys->orders.empty())) {
		throw InvalidInputException("Function \"%s\" is a %s. \"DISTINCT\", \"FILTER\", and \"ORDER BY\" are only "
		                            "applicable to aggregate functions.",
		                            function.function_name, CatalogTypeToString(func->type));
	}

	switch (func->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		if (function.IsLambdaFunction()) {
			return TryBindLambdaOrJson(function, depth, *func);
		}
		return BindFunction(function, func->Cast<ScalarFunctionCatalogEntry>(), depth);
	}
	case CatalogType::MACRO_ENTRY:
		// macro function
		return BindMacro(function, func->Cast<ScalarMacroCatalogEntry>(), depth, expr_ptr);
	default:
		// aggregate function
		return BindAggregate(function, func->Cast<AggregateFunctionCatalogEntry>(), depth);
	}
}

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func, idx_t depth) {
	// bind the children of the function expression
	ErrorData error;

	// bind of each child
	for (idx_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}

	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = BoundExpression::GetExpression(*function.children[i]);
		children.push_back(std::move(child));
	}

	FunctionBinder function_binder(context);
	auto result = function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}
	if (result->type == ExpressionType::BOUND_FUNCTION) {
		auto &bound_function = result->Cast<BoundFunctionExpression>();
		if (bound_function.function.stability == FunctionStability::CONSISTENT_WITHIN_QUERY) {
			binder.SetAlwaysRequireRebind();
		}
	}
	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindLambdaFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func,
                                                idx_t depth) {

	// scalar functions with lambdas can never be overloaded
	if (func.functions.functions.size() != 1) {
		return BindResult("This scalar function does not support lambdas!");
	}

	// get the callback function for the lambda parameter types
	auto &scalar_function = func.functions.functions.front();
	auto &bind_lambda_function = scalar_function.bind_lambda;
	if (!bind_lambda_function) {
		return BindResult("This scalar function does not support lambdas!");
	}

	if (function.children.size() != 2) {
		return BindResult("Invalid number of function arguments!");
	}
	D_ASSERT(function.children[1]->GetExpressionClass() == ExpressionClass::LAMBDA);

	// bind the list parameter
	ErrorData error;
	BindChild(function.children[0], depth, error);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}

	// get the logical type of the children of the list
	auto &list_child = BoundExpression::GetExpression(*function.children[0]);
	if (list_child->return_type.id() != LogicalTypeId::LIST && list_child->return_type.id() != LogicalTypeId::ARRAY &&
	    list_child->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child->return_type.id() != LogicalTypeId::UNKNOWN) {
		return BindResult("Invalid LIST argument during lambda function binding!");
	}

	LogicalType list_child_type = list_child->return_type.id();
	if (list_child->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child->return_type.id() != LogicalTypeId::UNKNOWN) {

		if (list_child->return_type.id() == LogicalTypeId::ARRAY) {
			list_child_type = ArrayType::GetChildType(list_child->return_type);
		} else {
			list_child_type = ListType::GetChildType(list_child->return_type);
		}
	}

	// bind the lambda parameter
	auto &lambda_expr = function.children[1]->Cast<LambdaExpression>();
	BindResult bind_lambda_result = BindExpression(lambda_expr, depth, list_child_type, &bind_lambda_function);

	if (bind_lambda_result.HasError()) {
		return BindResult(bind_lambda_result.error);
	}

	// successfully bound: replace the node with a BoundExpression
	auto alias = function.children[1]->alias;
	bind_lambda_result.expression->alias = alias;
	if (!alias.empty()) {
		bind_lambda_result.expression->alias = alias;
	}
	function.children[1] = make_uniq<BoundExpression>(std::move(bind_lambda_result.expression));

	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = BoundExpression::GetExpression(*function.children[i]);
		children.push_back(std::move(child));
	}

	// capture the (lambda) columns
	auto &bound_lambda_expr = children.back()->Cast<BoundLambdaExpression>();
	CaptureLambdaColumns(bound_lambda_expr, bound_lambda_expr.lambda_expr, &bind_lambda_function, list_child_type);

	FunctionBinder function_binder(context);
	unique_ptr<Expression> result =
	    function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}

	auto &bound_function_expr = result->Cast<BoundFunctionExpression>();
	D_ASSERT(bound_function_expr.children.size() == 2);

	// remove the lambda expression from the children
	auto lambda = std::move(bound_function_expr.children.back());
	bound_function_expr.children.pop_back();
	auto &bound_lambda = lambda->Cast<BoundLambdaExpression>();

	// push back (in reverse order) any nested lambda parameters so that we can later use them in the lambda
	// expression (rhs). This happens after we bound the lambda expression of this depth. So it is relevant for
	// correctly binding lambdas one level 'out'. Therefore, the current parameter count does not matter here.
	idx_t offset = 0;
	if (lambda_bindings) {
		for (idx_t i = lambda_bindings->size(); i > 0; i--) {

			auto &binding = (*lambda_bindings)[i - 1];
			D_ASSERT(binding.names.size() == binding.types.size());

			for (idx_t column_idx = binding.names.size(); column_idx > 0; column_idx--) {
				auto bound_lambda_param = make_uniq<BoundReferenceExpression>(binding.names[column_idx - 1],
				                                                              binding.types[column_idx - 1], offset);
				offset++;
				bound_function_expr.children.push_back(std::move(bound_lambda_param));
			}
		}
	}

	// push back the captures into the children vector
	for (auto &capture : bound_lambda.captures) {
		bound_function_expr.children.push_back(std::move(capture));
	}

	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                           idx_t depth) {
	return BindResult(BinderException(expr, UnsupportedAggregateMessage()));
}

BindResult ExpressionBinder::BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression) {
	return BindResult(BinderException(expr, UnsupportedUnnestMessage()));
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

optional_ptr<CatalogEntry> ExpressionBinder::GetCatalogEntry(CatalogType type, const string &catalog,
                                                             const string &schema, const string &name,
                                                             OnEntryNotFound on_entry_not_found,
                                                             QueryErrorContext &error_context) {
	return binder.GetCatalogEntry(type, catalog, schema, name, on_entry_not_found, error_context);
}

} // namespace duckdb
