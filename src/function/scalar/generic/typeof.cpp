#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

static void TypeOfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Value v(args.data[0].GetType().ToString());
	result.Reference(v);
}

void TypeOfFun::RegisterFunction(BuiltinFunctions &set) {
	auto fun = ScalarFunction("typeof", {LogicalType::ANY}, LogicalType::VARCHAR, TypeOfFunction);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
}

} // namespace duckdb
