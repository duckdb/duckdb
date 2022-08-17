#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
}

void HashFun::RegisterFunction(BuiltinFunctions &set) {
	auto hash_fun = ScalarFunction("hash", {LogicalType::ANY}, LogicalType::HASH, HashFunction);
	hash_fun.varargs = LogicalType::ANY;
	hash_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(hash_fun);
}

} // namespace duckdb
