#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
}

void HashFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("hash", {LogicalType::ANY}, LogicalType::HASH, HashFunction));
}

} // namespace duckdb
