#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void CardinalityFunction(DataChunk &args, ExpressionState &, Vector &result) {
	UnaryExecutor::Execute<list_entry_t, uint64_t>(args.data[0], result, args.size(),
	                                               [&](const list_entry_t &input) { return input.length; });
}

ScalarFunction CardinalityFun::GetFunction() {
	const auto key_type = LogicalType::TEMPLATE("K");
	const auto val_type = LogicalType::TEMPLATE("V");
	const auto map_type = LogicalType::MAP(key_type, val_type);

	auto sig = FunctionSignature().AddParameter("map", map_type).SetReturnType(LogicalTypeId::UBIGINT);

	auto fun = ScalarFunction("cardinality", std::move(sig), CardinalityFunction);

	return fun;
}

} // namespace duckdb
