#include <string>
#include <utility>

#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;

// Reverse of map_from_entries
static void MapEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	auto &map = args.data[0];
	if (map.GetType().id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result, count_t(count));
		return;
	}
	MapUtil::ReinterpretMap(result, map, count);
}

ScalarFunction MapEntriesFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	auto map_type = LogicalType::MAP(key_type, val_type);
	auto row_type = LogicalType::STRUCT({{"key", key_type}, {"value", val_type}});

	ScalarFunction fun({map_type}, LogicalType::LIST(row_type), MapEntriesFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
