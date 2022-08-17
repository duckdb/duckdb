//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar_macro_function.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/function/scalar_macro_function.hpp"

#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

ScalarMacroFunction::ScalarMacroFunction(unique_ptr<ParsedExpression> expression)
    : MacroFunction(MacroType::SCALAR_MACRO), expression(move(expression)) {
}

ScalarMacroFunction::ScalarMacroFunction(void) : MacroFunction(MacroType::SCALAR_MACRO) {
}

unique_ptr<MacroFunction> ScalarMacroFunction::Copy() {
	auto result = make_unique<ScalarMacroFunction>();
	result->expression = expression->Copy();
	CopyProperties(*result);

	return move(result);
}

void RemoveQualificationRecursive(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &col_ref = (ColumnRefExpression &)*expr;
		auto &col_names = col_ref.column_names;
		if (col_names.size() == 2 && col_names[0].find(DummyBinding::DUMMY_NAME) != string::npos) {
			col_names.erase(col_names.begin());
		}
	} else {
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [](unique_ptr<ParsedExpression> &child) { RemoveQualificationRecursive(child); });
	}
}

string ScalarMacroFunction::ToSQL(const string &schema, const string &name) {
	// In case of nested macro's we need to fix it a bit
	auto expression_copy = expression->Copy();
	RemoveQualificationRecursive(expression_copy);
	return MacroFunction::ToSQL(schema, name) + StringUtil::Format("(%s);", expression_copy->ToString());
}

} // namespace duckdb
