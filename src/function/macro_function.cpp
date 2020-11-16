#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

unique_ptr<Expression> MacroFunction::BindMacroFunction(Binder &binder, ExpressionBinder &expr_binder,
                                                        MacroFunctionCatalogEntry &function,
                                                        vector<unique_ptr<Expression>> arguments, string &error) {
	// verify correct number of arguments
	auto &macro_func = function.function;
	auto &parameters = macro_func->parameters;
	if (parameters.size() != arguments.size()) {
		error = StringUtil::Format(
		    "Macro function '%s(%s)' requires ", function.name,
		    StringUtil::Join(parameters, parameters.size(), ", ", [](const unique_ptr<ParsedExpression> &p) {
			    return ((ColumnRefExpression &)*p).column_name;
		    }));
		error += parameters.size() == 1 ? "a single argument" : StringUtil::Format("%i arguments", parameters.size());
		error += ", but ";
		error +=
		    arguments.size() == 1 ? "a single argument was" : StringUtil::Format("%i arguments were", arguments.size());
		error += " provided.";
		return nullptr;
	}

	// check for arguments with side-effects TODO: to support this, a projection must be pushed
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (arguments[i]->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
			auto &bound_func = (BoundFunctionExpression &)*arguments[i];
			if (bound_func.function.has_side_effects) {
				error = StringUtil::Format(
				    "Arguments with side-effects are not supported ('%s' was supplied). As a "
				    "workaround, try creating a CTE that evaluates the argument with side-effects.",
				    bound_func.function.name);
				return nullptr;
			}
		}
	}

	// we do not support non-constant parameters that are used in subqueries TODO: finish this
	if (macro_func->expression->GetExpressionClass() == ExpressionClass::SUBQUERY) {
        auto &sqe = (SubqueryExpression &)*macro_func->expression;
        // check children
	}
    ParsedExpressionIterator::EnumerateChildren(*macro_func->expression, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
        // check children
        return child;
    });

	// create macro_binder in binder to bind parameters to arguments
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < parameters.size(); i++) {
		types.push_back(arguments[i]->return_type);
		auto &param = (ColumnRefExpression &)*parameters[i];
		names.push_back(param.column_name);
	}
	binder.macro_binding = make_shared<MacroBinding>(types, names);
	binder.macro_binding->arguments = move(arguments);

	// TODO: we bind arguments at depth = 0, then add them to the macro_binding
	// TODO: however, parameters may appear within subqueries in the function, at a lower depth
	// TODO: now, when these parameters to their arguments, the binding no longer makes sense

	// TODO: correlated columns should be bound at the right depth
	// TODO: however, we cannot call expr_binder.Bind with depth != 0 due to an assertion

	// TODO: instead, we could copy over the unbound expression, and then bind the whole thing at once
	// TODO: this way, the arguments get bound at the right depth
    // TODO: this is what I was doing before, but oh well

	// TODO: this may cause other problems
	// TODO: it might be better to disallow parameters in subqueries that are not constants

	// now we perform the binding
	auto parsed_expression = macro_func->expression->Copy();
	auto result = expr_binder.Bind(parsed_expression);
//    auto bound_expr = (BoundExpression *)parsed_expression.get();
//    unique_ptr<Expression> result = move(bound_expr->expr);

	// delete the macro binding so that it cannot effect bindings outside of the macro call
	binder.macro_binding.reset();

	return result;
}

} // namespace duckdb
