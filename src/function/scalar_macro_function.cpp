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

} // namespace duckdb
