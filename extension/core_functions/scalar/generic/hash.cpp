#include "core_functions/scalar/generic_functions.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction HashFun::GetFunction() {
	auto hash_fun = ScalarFunction({LogicalType::ANY}, LogicalType::HASH, HashFunction);
	hash_fun.varargs = LogicalType::ANY;
	hash_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return hash_fun;
}

} // namespace duckdb
