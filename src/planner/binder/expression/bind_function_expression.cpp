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

BindResult ExpressionBinder::TryBindLambdaOrJson(FunctionExpression &function, idx_t depth, CatalogEntry &func,
                                                 const LambdaSyntaxType syntax_type) {
	if (syntax_type == LambdaSyntaxType::LAMBDA_KEYWORD) {
		// lambda x: x + 1 syntax.
		return BindLambdaFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	}

	auto &config = ClientConfig::GetConfig(context);
	auto setting = config.lambda_syntax;
	bool invalid_syntax =
	    setting == LambdaSyntax::DISABLE_SINGLE_ARROW && syntax_type == LambdaSyntaxType::SINGLE_ARROW;
	const string msg = "Deprecated lambda arrow (->) detected. Please transition to the new lambda syntax, "
	                   "i.e.., lambda x, i: x + i, before DuckDB's next release. \n"
	                   "Use SET lambda_syntax='ENABLE_SINGLE_ARROW' to revert to the deprecated behavior. \n"
	                   "For more information, see https://duckdb.org/docs/stable/sql/functions/lambda.html.";

	BindResult lambda_bind_result;
	ErrorData error;
	try {
		lambda_bind_result = BindLambdaFunction(function, func.Cast<ScalarFunctionCatalogEntry>(), depth);
	} catch (const std::exception &ex) {
		error = ErrorData(ex);
	}

	if (error.HasError() && error.Type() == ExceptionType::PARAMETER_NOT_RESOLVED && invalid_syntax) {
		ErrorData deprecation_error(ExceptionType::BINDER, msg);
		deprecation_error.Throw();
	} else if (error.HasError()) {
		error.Throw();
	}

	if (!lambda_bind_result.HasError()) {
		if (!invalid_syntax) {
			return lambda_bind_result;
		}
		return BindResult(msg);
	}
	if (StringUtil::Contains(lambda_bind_result.error.RawMessage(), "Deprecated lambda arrow (->) detected.")) {
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

optional_ptr<CatalogEntry> ExpressionBinder::BindAndQualifyFunction(FunctionExpression &function, bool allow_throw) {
	D_ASSERT(!IsUnnestFunction(function.function_name));
	// lookup the function in the catalog
	QueryErrorContext error_context(function.GetQueryLocation());
	binder.BindSchemaOrCatalog(function.catalog, function.schema);

	EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function.function_name, error_context);
	auto func = GetCatalogEntry(function.catalog, function.schema, function_lookup, OnEntryNotFound::RETURN_NULL);
	if (!func) {
		// function was not found - check if we this is a table function
		EntryLookupInfo table_function_lookup(CatalogType::TABLE_FUNCTION_ENTRY, function.function_name, error_context);
		auto table_func =
		    GetCatalogEntry(function.catalog, function.schema, table_function_lookup, OnEntryNotFound::RETURN_NULL);
		if (table_func) {
			if (!allow_throw) {
				return func;
			}
			throw BinderException(function,
			                      "Function \"%s\" is a table function but it was used as a scalar function. This "
			                      "function has to be called in a FROM clause (similar to a table).",
			                      function.function_name);
		}
		// not a table function - check if the schema is set
		if (!function.schema.empty()) {
			// the schema is set - check if we can turn this the schema into a column ref
			// does this function exist in the system catalog?
			func = GetCatalogEntry(INVALID_CATALOG, INVALID_SCHEMA, function_lookup, OnEntryNotFound::RETURN_NULL);
			if (func) {
				// the function exists in the system catalog - turn this into a dot call
				ErrorData error;
				unique_ptr<ColumnRefExpression> colref;
				if (function.catalog.empty()) {
					colref = make_uniq<ColumnRefExpression>(function.schema);
				} else {
					colref = make_uniq<ColumnRefExpression>(function.schema, function.catalog);
				}
				auto new_colref = QualifyColumnName(*colref, error);
				if (error.HasError()) {
					// could not find the column - try to qualify the alias
					if (!QualifyColumnAlias(*colref)) {
						if (!allow_throw) {
							return func;
						}
						// no alias found either - throw
						error.Throw();
					}
				}
				// we can! transform this into a function call on the column
				// i.e. "x.lower()" becomes "lower(x)"
				function.children.insert(function.children.begin(), std::move(colref));
				function.catalog = INVALID_CATALOG;
				function.schema = INVALID_SCHEMA;
			}
		}
		// rebind the function
		if (!func) {
			const auto on_entry_not_found =
			    allow_throw ? OnEntryNotFound::THROW_EXCEPTION : OnEntryNotFound::RETURN_NULL;
			func = GetCatalogEntry(function.catalog, function.schema, function_lookup, on_entry_not_found);
		}
	}

	return func;
}

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth,
                                            unique_ptr<ParsedExpression> &expr_ptr) {
	auto func = BindAndQualifyFunction(function, true);

	if (func->type != CatalogType::AGGREGATE_FUNCTION_ENTRY &&
	    (function.distinct || function.filter || !function.order_bys->orders.empty())) {
		throw InvalidInputException("Function \"%s\" is a %s. \"DISTINCT\", \"FILTER\", and \"ORDER BY\" are only "
		                            "applicable to aggregate functions.",
		                            function.function_name, CatalogTypeToString(func->type));
	}

	switch (func->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		auto child = function.IsLambdaFunction();
		if (child) {
			auto syntax_type = child->Cast<LambdaExpression>().syntax_type;
			return TryBindLambdaOrJson(function, depth, *func, syntax_type);
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
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES ||
	    binder.GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = BoundExpression::GetExpression(*function.children[i]);
		children.push_back(std::move(child));
	}

	FunctionBinder function_binder(binder);
	auto result = function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}
	if (result->GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
		auto &bound_function = result->Cast<BoundFunctionExpression>();
		if (bound_function.function.GetStability() == FunctionStability::CONSISTENT_WITHIN_QUERY) {
			binder.SetAlwaysRequireRebind();
		}
	}
	return BindResult(std::move(result));
}

BindResult ExpressionBinder::BindLambdaFunction(FunctionExpression &function, ScalarFunctionCatalogEntry &func,
                                                idx_t depth) {
	// get the callback function for the lambda parameter types
	auto &scalar_function = func.functions.functions.front();
	auto &bind_lambda_function = scalar_function.bind_lambda;
	if (!bind_lambda_function) {
		return BindResult("This scalar function does not support lambdas!");
	}

	// the first child is the list, the second child is the lambda expression
	constexpr idx_t list_idx = 0;
	constexpr idx_t lambda_expr_idx = 1;
	D_ASSERT(function.children[lambda_expr_idx]->GetExpressionClass() == ExpressionClass::LAMBDA);

	vector<LogicalType> function_child_types;

	// bind the list
	ErrorData error;
	for (idx_t i = 0; i < function.children.size(); i++) {
		if (i == lambda_expr_idx) {
			function_child_types.push_back(LogicalType::LAMBDA);
			continue;
		}

		if (function.children[i]->GetExpressionClass() == ExpressionClass::LAMBDA) {
			return BindResult("No function matches the given name and argument types: '" + function.ToString() +
			                  "'. You might need to add explicit type casts.");
		}

		BindChild(function.children[i], depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}

		const auto &child = BoundExpression::GetExpression(*function.children[i]);
		function_child_types.push_back(child->return_type);
	}

	// get the logical type of the children of the list
	auto &list_child = BoundExpression::GetExpression(*function.children[list_idx]);
	if (list_child->return_type.id() != LogicalTypeId::LIST && list_child->return_type.id() != LogicalTypeId::ARRAY &&
	    list_child->return_type.id() != LogicalTypeId::SQLNULL &&
	    list_child->return_type.id() != LogicalTypeId::UNKNOWN) {
		return BindResult("Invalid LIST argument during lambda function binding!");
	}

	// bind the lambda parameter
	auto &lambda_expr = function.children[lambda_expr_idx]->Cast<LambdaExpression>();
	BindResult bind_lambda_result = BindExpression(lambda_expr, depth, function_child_types, &bind_lambda_function);

	if (bind_lambda_result.HasError()) {
		return BindResult(bind_lambda_result.error);
	}

	// successfully bound: replace the node with a BoundExpression
	auto alias = function.children[lambda_expr_idx]->GetAlias();
	bind_lambda_result.expression->SetAlias(alias);
	if (!alias.empty()) {
		bind_lambda_result.expression->SetAlias(alias);
	}
	function.children[lambda_expr_idx] = make_uniq<BoundExpression>(std::move(bind_lambda_result.expression));

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
	auto &bound_lambda_expr = children[lambda_expr_idx]->Cast<BoundLambdaExpression>();
	CaptureLambdaColumns(bound_lambda_expr, bound_lambda_expr.lambda_expr, &bind_lambda_function, function_child_types);

	FunctionBinder function_binder(binder);
	unique_ptr<Expression> result =
	    function_binder.BindScalarFunction(func, std::move(children), error, function.is_operator, &binder);
	if (!result) {
		error.AddQueryLocation(function);
		error.Throw();
	}

	auto &bound_function_expr = result->Cast<BoundFunctionExpression>();

	// remove the lambda expression from the children
	auto lambda = std::move(bound_function_expr.children[lambda_expr_idx]);
	bound_function_expr.children.erase_at(lambda_expr_idx);
	auto &bound_lambda = lambda->Cast<BoundLambdaExpression>();

	// push back (in reverse order) any nested lambda parameters so that we can later use them in the lambda
	// expression (rhs). This happens after we bound the lambda expression of this depth. So it is relevant for
	// correctly binding lambdas one level 'out'. Therefore, the current parameter count does not matter here.
	idx_t offset = 0;
	if (lambda_bindings) {
		for (idx_t i = lambda_bindings->size(); i > 0; i--) {
			auto &binding = (*lambda_bindings)[i - 1];
			auto &column_names = binding.GetColumnNames();
			auto &column_types = binding.GetColumnTypes();
			D_ASSERT(column_names.size() == column_types.size());

			for (idx_t column_idx = column_names.size(); column_idx > 0; column_idx--) {
				auto bound_lambda_param = make_uniq<BoundReferenceExpression>(column_names[column_idx - 1],
				                                                              column_types[column_idx - 1], offset);
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
	return BindUnsupportedExpression(expr, depth, UnsupportedAggregateMessage());
}

BindResult ExpressionBinder::BindUnnest(FunctionExpression &expr, idx_t depth, bool root_expression) {
	return BindUnsupportedExpression(expr, depth, UnsupportedUnnestMessage());
}

void ExpressionBinder::ThrowIfUnnestInLambda(const ColumnBinding &column_binding) {
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

optional_ptr<CatalogEntry> ExpressionBinder::GetCatalogEntry(const string &catalog, const string &schema,
                                                             const EntryLookupInfo &lookup_info,
                                                             OnEntryNotFound on_entry_not_found) {
	return binder.GetCatalogEntry(catalog, schema, lookup_info, on_entry_not_found);
}

} // namespace duckdb
