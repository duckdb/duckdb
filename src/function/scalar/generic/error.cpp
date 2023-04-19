#include "duckdb/function/scalar/generic_functions.hpp"
#include <iostream>

namespace duckdb {

struct ErrorOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		throw Exception(input.GetString());
	}
};

void ErrorFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = ScalarFunction("error", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                          ScalarFunction::UnaryFunction<string_t, bool, ErrorOperator>);
	// Set the function with side effects to avoid the optimization.
	fun.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	set.AddFunction(fun);
}

} // namespace duckdb
