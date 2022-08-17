#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth,
                                            unique_ptr<ParsedExpression> *expr_ptr) {
	// lookup the function in the catalog
	QueryErrorContext error_context(binder.root_statement, function.query_location);

	if (function.function_name == "unnest" || function.function_name == "unlist") {
		// special case, not in catalog
		// TODO make sure someone does not create such a function OR
		// have unnest live in catalog, too
		return BindUnnest(function, depth);
	}
	auto &catalog = Catalog::GetCatalog(context);
	auto func = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function.schema, function.function_name,
	                             false, error_context);

	switch (func->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		// scalar function

		// check for lambda parameters, ignore ->> operator (JSON extension)
		if (function.function_name != "->>") {
			for (auto &child : function.children) {
				if (child->expression_class == ExpressionClass::LAMBDA) {
					return BindLambdaFunction(function, (ScalarFunctionCatalogEntry *)func, depth);
				}
			}
		}

		// other scalar function
		return BindFunction(function, (ScalarFunctionCatalogEntry *)func, depth);

	case CatalogType::MACRO_ENTRY:
		// macro function
		return BindMacro(function, (ScalarMacroCatalogEntry *)func, depth, expr_ptr);
	default:
		// aggregate function
		return BindAggregate(function, (AggregateFunctionCatalogEntry *)func, depth);
	}
}

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func, idx_t depth) {

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
		return BindResult(make_unique<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		D_ASSERT(child.expr);
		children.push_back(move(child.expr));
	}
	unique_ptr<Expression> result =
	    ScalarFunction::BindScalarFunction(context, *func, move(children), error, function.is_operator, &binder);
	if (!result) {
		throw BinderException(binder.FormatError(function, error));
	}
	return BindResult(move(result));
}

BindResult ExpressionBinder::BindLambdaFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func,
                                                idx_t depth) {

	// bind the children of the function expression
	string error;

	if (function.children.size() != 2) {
		throw BinderException("Invalid function arguments!");
	}
	D_ASSERT(function.children[1]->GetExpressionClass() == ExpressionClass::LAMBDA);

	// bind the list parameter
	BindChild(function.children[0], depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}

	// get the logical type of the children of the list
	auto &list_child = (BoundExpression &)*function.children[0];

	if (list_child.expr->return_type.id() != LogicalTypeId::LIST &&
	    list_child.expr->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child.expr->return_type.id() != LogicalTypeId::UNKNOWN) {
		throw BinderException(" Invalid LIST argument to " + function.function_name + "!");
	}

	LogicalType list_child_type = list_child.expr->return_type.id();
	if (list_child.expr->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child.expr->return_type.id() != LogicalTypeId::UNKNOWN) {
		list_child_type = ListType::GetChildType(list_child.expr->return_type);
	}

	// bind the lambda parameter
	auto &lambda_expr = (LambdaExpression &)*function.children[1];
	BindResult bind_lambda_result = BindExpression(lambda_expr, depth, true, list_child_type);

	if (bind_lambda_result.HasError()) {
		error = bind_lambda_result.error;
	} else {
		// successfully bound: replace the node with a BoundExpression
		auto alias = function.children[1]->alias;
		function.children[1] = make_unique<BoundExpression>(move(bind_lambda_result.expression));
		auto be = (BoundExpression *)function.children[1].get();
		D_ASSERT(be);
		be->alias = alias;
		if (!alias.empty()) {
			be->expr->alias = alias;
		}
	}

	if (!error.empty()) {
		return BindResult(error);
	}
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		return BindResult(make_unique<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		D_ASSERT(child.expr);
		children.push_back(move(child.expr));
	}

	// capture the (lambda) columns
	auto &bound_lambda_expr = (BoundLambdaExpression &)*children.back();
	CaptureLambdaColumns(bound_lambda_expr.captures, list_child_type, bound_lambda_expr.lambda_expr,
	                     children[0]->alias);

	unique_ptr<Expression> result =
	    ScalarFunction::BindScalarFunction(context, *func, move(children), error, function.is_operator, &binder);
	if (!result) {
		throw BinderException(binder.FormatError(function, error));
	}

	// remove the lambda expression from the children
	auto &bound_function_expr = (BoundFunctionExpression &)*result;
	auto lambda = move(bound_function_expr.children.back());
	bound_function_expr.children.pop_back();
	auto &bound_lambda = (BoundLambdaExpression &)*lambda;

	// push back the captures into the children vector and the correct return types into the bound_function arguments
	for (auto &capture : bound_lambda.captures) {
		bound_function_expr.function.arguments.push_back(capture->return_type);
		bound_function_expr.children.push_back(move(capture));
	}

	return BindResult(move(result));
}

BindResult ExpressionBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry *function,
                                           idx_t depth) {
	return BindResult(binder.FormatError(expr, UnsupportedAggregateMessage()));
}

BindResult ExpressionBinder::BindUnnest(FunctionExpression &expr, idx_t depth) {
	return BindResult(binder.FormatError(expr, UnsupportedUnnestMessage()));
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

} // namespace duckdb
