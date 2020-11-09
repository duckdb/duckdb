#include "duckdb/function/macro_function.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

MacroFunction::MacroFunction(unique_ptr<ParsedExpression> expression) : expression(move(expression)) {
}

unique_ptr<Expression> MacroFunction::BindMacroFunction(ExpressionBinder &binder, MacroFunctionCatalogEntry &function,
                                                        vector<unique_ptr<ParsedExpression>> parsed_children,
                                                        vector<unique_ptr<Expression>> bound_children, string &error) {
	// TODO: to support arguments with side-effects a projection must be pushed
	for (auto &child : bound_children) {
		if (!child->IsFoldable()) {
			throw BinderException("Arguments with side-effects not yet supported \"%s\"", child->ToString());
		}
	}

	// replace arguments with those that were supplied
	auto &macro_func = function.function;
	auto parsed_expression = macro_func->expression->Copy();
	ParsedExpressionIterator::EnumerateChildren(
	    *parsed_expression, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
		    for (idx_t i = 0; i < parsed_children.size(); i++) {
			    if (child->Equals(macro_func->arguments[i].get())) {
				    return parsed_children[i]->Copy();
			    }
		    }
		    return child;
	    });

	// now we perform the binding
	LogicalType return_type;
	return binder.Bind(parsed_expression, &return_type);
}

} // namespace duckdb
