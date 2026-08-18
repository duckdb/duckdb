#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// combine the hash of the first value with the hash of every value collected by "*values"
	const auto count = args.size();
	VectorOperations::Hash(args.data[0], result, count);
	for (auto &value : ArgumentPack::GetInput(args.data[1])) {
		VectorOperations::CombineHash(result, value, count);
	}
}

ScalarFunction HashFun::GetFunction() {
	return ScalarFunction("hash", FunctionSignature()
	                                  .AddParameter("arg", LogicalType::ANY)
	                                  .AddVarPositionalParameter("args", LogicalType::ANY)
	                                  .SetReturnType(LogicalType::HASH))
	    .SetFunctionCallback(HashFunction)
	    .SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
}

} // namespace duckdb
