#include "duckdb/function/scalar/uuid_functions.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static void GenerateUUIDFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 0);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<hugeint_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = UUID::Random();
	}
}

void UUIDFun::RegisterFunction(BuiltinFunctions &set) {
	// generate a random uuid
	set.AddFunction(ScalarFunction("gen_random_uuid", {}, LogicalType::UUID, GenerateUUIDFunction, true));
}

} // namespace duckdb
