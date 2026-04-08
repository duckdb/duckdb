#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
class Vector;
struct ExpressionState;

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	args.Hash(result);
}

ScalarFunction HashFun::GetFunction() {
	auto hash_fun = ScalarFunction({LogicalType::ANY}, LogicalType::HASH, HashFunction);
	hash_fun.varargs = LogicalType::ANY;
	hash_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return hash_fun;
}

} // namespace duckdb
