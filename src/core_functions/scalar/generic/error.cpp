#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include <iostream>

namespace duckdb {

struct ErrorOperator {
	template <class TA, class TR>
	static inline TR Operation(const TA &input) {
		throw Exception(input.GetString());
	}
};

ScalarFunction ErrorFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                          ScalarFunction::UnaryFunction<string_t, bool, ErrorOperator>);
	// Set the function with side effects to avoid the optimization.
	fun.side_effects = FunctionSideEffects::HAS_SIDE_EFFECTS;
	return fun;
}

} // namespace duckdb
