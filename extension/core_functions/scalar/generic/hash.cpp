#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// combine the hash of the first value with the hash of every value collected by "*values"
	const auto count = args.size();
	VectorOperations::Hash(args.data[0], result, count);
	for (auto &value : StructVector::GetEntries(args.data[1])) {
		VectorOperations::CombineHash(result, value, count);
	}
}

ScalarFunction HashFun::GetFunction() {
	FunctionSignature signature;
	signature.AddParameter("value", LogicalType::ANY);
	signature.AddVarPositionalParameter("values", LogicalType::ANY);
	signature.SetReturnType(LogicalType::HASH);
	auto hash_fun = ScalarFunction("hash", std::move(signature), HashFunction);
	hash_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return hash_fun;
}

} // namespace duckdb
