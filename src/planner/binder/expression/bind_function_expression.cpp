#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {
using namespace std;

BindResult ExpressionBinder::BindExpression(FunctionExpression &function, idx_t depth) {
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
		return BindFunction(function, (ScalarFunctionCatalogEntry *)func, depth);
	case CatalogType::MACRO_ENTRY:
		// macro function
		return BindMacro(function);
	default:
		// aggregate function
		return BindAggregate(function, (AggregateFunctionCatalogEntry *)func, depth);
	}
}

BindResult ExpressionBinder::BindFunction(FunctionExpression &function, ScalarFunctionCatalogEntry *func, idx_t depth) {
	// bind the children of the function expression
	string error;
	for (idx_t i = 0; i < function.children.size(); i++) {
		BindChild(function.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// all children bound successfully
	// extract the children and types
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < function.children.size(); i++) {
		auto &child = (BoundExpression &)*function.children[i];
		children.push_back(move(child.expr));
	}
	// special binder-only functions
	// FIXME: these shouldn't be special
	if (function.function_name == "alias") {
		if (children.size() != 1) {
			throw BinderException(binder.FormatError(function, "alias function expects a single argument"));
		}
		// alias function: returns the alias of the current expression, or the name of the child
		string alias = !function.alias.empty() ? function.alias : children[0]->GetName();
		return BindResult(make_unique<BoundConstantExpression>(Value(alias)));
	} else if (function.function_name == "typeof") {
		if (children.size() != 1) {
			throw BinderException(binder.FormatError(function, "typeof function expects a single argument"));
		}
		// typeof function: returns the type of the child expression
		string type = children[0]->return_type.ToString();
		return BindResult(make_unique<BoundConstantExpression>(Value(type)));
	}
	unique_ptr<Expression> result =
	    ScalarFunction::BindScalarFunction(context, *func, move(children), error, function.is_operator);
	if (!result) {
		throw BinderException(binder.FormatError(function, error));
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

unique_ptr<ParsedExpression> ExpressionBinder::UnfoldMacroRecursive(unique_ptr<ParsedExpression> expr) {
	auto macro_binding = make_unique<MacroBinding>(vector<LogicalType>(), vector<string>(), string());
	return UnfoldMacroRecursive(move(expr), *macro_binding);
}

unique_ptr<ParsedExpression> ExpressionBinder::UnfoldMacroRecursive(unique_ptr<ParsedExpression> expr,
                                                                    MacroBinding &macro_binding) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// if expr is a parameter, replace it with its argument
		auto &colref = (ColumnRefExpression &)*expr;
		if (colref.table_name.empty() && macro_binding.HasMatchingBinding(colref.column_name)) {
			expr = macro_binding.ParamToArg(colref);
			return UnfoldMacroRecursive(move(expr), macro_binding);
		}
		return expr;
	}
	case ExpressionClass::FUNCTION: {
		auto &function_expr = (FunctionExpression &)*expr;
		if (function_expr.is_operator)
			break;

		// if expr is a macro function, unfold it
		QueryErrorContext error_context(binder.root_statement, function_expr.query_location);
		auto &catalog = Catalog::GetCatalog(context);
		auto func = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function_expr.schema,
		                             function_expr.function_name, false, error_context);
		if (func->type == CatalogType::MACRO_ENTRY) {
			auto &macro_func = (MacroFunctionCatalogEntry &)*func;
			string error = MacroFunction::ValidateArguments(context, error_context, macro_func, function_expr);
			if (!error.empty())
				throw BinderException(binder.FormatError(*expr, error));

			// create macro_binding to bind this macro's parameters to its arguments
			vector<LogicalType> types;
			vector<string> names;
			for (idx_t i = 0; i < macro_func.function->parameters.size(); i++) {
				types.push_back(LogicalType::SQLNULL);
				auto &param = (ColumnRefExpression &)*macro_func.function->parameters[i];
				names.push_back(param.column_name);
			}
			auto new_macro_binding = make_unique<MacroBinding>(types, names, func->name);
			new_macro_binding->arguments = move(function_expr.children);

			expr = macro_func.function->expression->Copy();
			return UnfoldMacroRecursive(move(expr), *new_macro_binding);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		// replacing parameters within a subquery is slightly different
		auto &sqe = (SubqueryExpression &)*expr;
		if (sqe.subquery->node->type != QueryNodeType::SELECT_NODE) {
			// TODO: throw an error
		}
		auto &sel_node = (SelectNode &)*sqe.subquery->node;
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			sel_node.select_list[i] = UnfoldMacroRecursive(move(sel_node.select_list[i]), macro_binding);
		}
		for (idx_t i = 0; i < sel_node.groups.size(); i++) {
			sel_node.groups[i] = UnfoldMacroRecursive(move(sel_node.groups[i]), macro_binding);
		}
		if (sel_node.where_clause != nullptr)
			sel_node.where_clause = UnfoldMacroRecursive(move(sel_node.where_clause), macro_binding);
		if (sel_node.having != nullptr)
			sel_node.having = UnfoldMacroRecursive(move(sel_node.having), macro_binding);
		break;
	}
	default: // fall through
		break;
	}
	// unfold child expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
		    return UnfoldMacroRecursive(move(child), macro_binding);
	    });
	return expr;
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &expr) {
	string error;
	auto unfolded_expr = UnfoldMacroRecursive(expr.Copy());
	return BindExpression(*unfolded_expr, 0, true);
}

string ExpressionBinder::UnsupportedAggregateMessage() {
	return "Aggregate functions are not supported here";
}

string ExpressionBinder::UnsupportedUnnestMessage() {
	return "UNNEST not supported here";
}

} // namespace duckdb
