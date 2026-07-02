#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void MapFromEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	MapUtil::ReinterpretMap(result, args.data[0], count);
	MapVector::MapConversionVerify(result, count);
	result.Verify(count);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction MapFromEntriesFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	auto map_type = LogicalType::MAP(key_type, val_type);
	auto row_type = LogicalType::STRUCT({{"", key_type}, {"", val_type}});

	ScalarFunction fun({LogicalType::LIST(row_type)}, map_type, MapFromEntriesFunction);
	fun.SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING);

	fun.SetFallible();
	return fun;
}

} // namespace duckdb
