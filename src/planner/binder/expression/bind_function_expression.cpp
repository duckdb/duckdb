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

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth,
                                            unique_ptr<ParsedExpression> &expr_ptr) {
	// lookup the function in the catalog
	QueryErrorContext error_context(binder.root_statement, function.query_location);
	auto func = Catalog::GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function.catalog, function.schema,
	                              function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
	if (!func) {
		// function was not found - check if we this is a table function
		auto table_func =
		    Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, function.catalog, function.schema,
		                      function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
		if (table_func) {
			throw BinderException(binder.FormatError(
			    function,
			    StringUtil::Format("Function \"%s\" is a table function but it was used as a scalar function. This "
			                       "function has to be called in a FROM clause (similar to a table).",
			                       function.function_name)));
		}
		// not a table function - check if the schema is set
		if (!function.schema.empty()) {
			// the schema is set - check if we can turn this the schema into a column ref
			string error;
			unique_ptr<ColumnRefExpression> colref;
			if (function.catalog.empty()) {
				colref = make_uniq<ColumnRefExpression>(function.schema);
			} else {
				colref = make_uniq<ColumnRefExpression>(function.schema, function.catalog);
			}
			auto new_colref = QualifyColumnName(*colref, error);
			bool is_col = error.empty() ? true : false;
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
		func = Catalog::GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function.catalog, function.schema,
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
		// scalar function

		// check for lambda parameters, ignore ->> operator (JSON extension)
		bool try_bind_lambda = false;
		if (function.function_name != "->>") {
			for (auto &child : function.children) {
				if (child->expression_class == ExpressionClass::LAMBDA) {
					try_bind_lambda = true;
				}
			}
		}

		if (try_bind_lambda) {
			auto result = BindLambdaFunction(function, func->Cast<ScalarFunctionCatalogEntry>(), depth);
			if (!result.HasError()) {
				// Lambda bind successful
				return result;
			}
		}

		// other scalar function
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
	string error;

	// bind of each child
	for (idx_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}

	if (!error.empty()) {
		return BindResult(error);
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
	unique_ptr<Expression> result =
	    function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		throw BinderException(binder.FormatError(function, error));
	}
	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindLambdaFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func,
                                                idx_t depth) {

	// bind the children of the function expression
	string error;

	if (function.children.size() != 2) {
		return BindResult("Invalid function arguments!");
	}
	D_ASSERT(function.children[1]->GetExpressionClass() == ExpressionClass::LAMBDA);

	// bind the list parameter
	BindChild(function.children[0], depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}

	// get the logical type of the children of the list
	auto &list_child = BoundExpression::GetExpression(*function.children[0]);
	if (list_child->return_type.id() != LogicalTypeId::LIST && list_child->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child->return_type.id() != LogicalTypeId::UNKNOWN) {
		return BindResult(" Invalid LIST argument to " + function.function_name + "!");
	}

	LogicalType list_child_type = list_child->return_type.id();
	if (list_child->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child->return_type.id() != LogicalTypeId::UNKNOWN) {
		list_child_type = ListType::GetChildType(list_child->return_type);
	}

	// bind the lambda parameter
	auto &lambda_expr = function.children[1]->Cast<LambdaExpression>();
	BindResult bind_lambda_result = BindExpression(lambda_expr, depth, true, list_child_type);

	if (bind_lambda_result.HasError()) {
		error = bind_lambda_result.error;
	} else {
		// successfully bound: replace the node with a BoundExpression
		auto alias = function.children[1]->alias;
		bind_lambda_result.expression->alias = alias;
		if (!alias.empty()) {
			bind_lambda_result.expression->alias = alias;
		}
		function.children[1] = make_uniq<BoundExpression>(std::move(bind_lambda_result.expression));
	}

	if (!error.empty()) {
		return BindResult(error);
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

	// capture the (lambda) columns
	auto &bound_lambda_expr = children.back()->Cast<BoundLambdaExpression>();
	CaptureLambdaColumns(bound_lambda_expr.captures, list_child_type, bound_lambda_expr.lambda_expr);

	FunctionBinder function_binder(context);
	unique_ptr<Expression> result =
	    function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		throw BinderException(binder.FormatError(function, error));
	}

	auto &bound_function_expr = result->Cast<BoundFunctionExpression>();
	D_ASSERT(bound_function_expr.children.size() == 2);

	// remove the lambda expression from the children
	auto lambda = std::move(bound_function_expr.children.back());
	bound_function_expr.children.pop_back();
	auto &bound_lambda = lambda->Cast<BoundLambdaExpression>();

	// push back (in reverse order) any nested lambda parameters so that we can later use them in the lambda expression
	// (rhs)
	if (lambda_bindings) {
		for (idx_t i = lambda_bindings->size(); i > 0; i--) {

			idx_t lambda_index = lambda_bindings->size() - i + 1;
			auto &binding = (*lambda_bindings)[i - 1];

			D_ASSERT(binding.names.size() == 1);
			D_ASSERT(binding.types.size() == 1);

			auto bound_lambda_param =
			    make_uniq<BoundReferenceExpression>(binding.names[0], binding.types[0], lambda_index);
			bound_function_expr.children.push_back(std::move(bound_lambda_param));
		}
	}

	// push back the captures into the children vector and the correct return types into the bound_function arguments
	for (auto &capture : bound_lambda.captures) {
		bound_function_expr.children.push_back(std::move(capture));
	}

	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                           idx_t depth) {
	return BindResult(binder.FormatError(expr, UnsupportedAggregateMessage()));
}

BindResult ExpressionBinder::BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression) {
	return BindResult(binder.FormatError(expr, UnsupportedUnnestMessage()));
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

} // namespace duckdb
