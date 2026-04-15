#include "duckdb/common/vector/map_vector.hpp"
#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void MapKeyValueFunction(DataChunk &args, ExpressionState &state, Vector &result,
                                Vector &(*get_child_vector)(Vector &)) {
	auto &map = args.data[0];

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	if (map.GetType().id() == LogicalTypeId::SQLNULL) {
		ConstantVector::SetNull(result);
		return;
	}
	auto count = args.size();
	map.Flatten(count);

	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	auto &child = get_child_vector(map);

	auto &entries = ListVector::GetEntry(result);
	entries.Reference(child);

	FlatVector::SetData(result, FlatVector::GetDataMutable(map), count);
	FlatVector::SetValidity(result, FlatVector::Validity(map));
	auto list_size = ListVector::GetListSize(map);
	ListVector::SetListSize(result, list_size);
	result.Verify(count);
}

static void MapKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetKeys);
}

static void MapValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetValues);
}

ScalarFunction MapKeysFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction function({LogicalType::MAP(key_type, val_type)}, LogicalType::LIST(key_type), MapKeysFunction);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	function.SetFallible();
	return function;
}

ScalarFunction MapValuesFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction function({LogicalType::MAP(key_type, val_type)}, LogicalType::LIST(val_type), MapValuesFunction);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	function.SetFallible();
	return function;
}

} // namespace duckdb
