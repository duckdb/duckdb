#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

// Reverse of map_from_entries
static void MapEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	idx_t count = args.size();

	// Get the arguments vector
	auto &map = args.data[0];
	auto &map_keys = MapVector::GetKeys(map);
	auto &map_values = MapVector::GetValues(map);

	UnifiedVectorFormat map_keys_data;
	UnifiedVectorFormat map_data;
	map_keys.ToUnifiedFormat(count, map_keys_data);
	map.ToUnifiedFormat(count, map_data);

	auto &result_struct = ListVector::GetEntry(result);
	auto &result_fields = StructVector::GetEntries(result_struct);
	auto &result_keys = *result_fields[0];
	auto &result_values = *result_fields[1];

	D_ASSERT(result_keys.GetType().id() == ListType::GetChildType(map_keys.GetType()).id());
	D_ASSERT(result_values.GetType().id() == ListType::GetChildType(map_values.GetType()).id());

	// Reference the map's keys and values, so we can do a zero-copy
	// This might be a problem if the maps values get altered in this query, but they *should* be immutable
	// Or if the resulting struct vectors get altered, it could have an effect on the projection results of the map
	result_keys.Reference(ListVector::GetEntry(map_keys));
	result_values.Reference(ListVector::GetEntry(map_values));

	// Reference the data for the list_entry_t's
	D_ASSERT(result_struct.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_keys_data.data);
	FlatVector::SetValidity(result, map_data.validity);

	auto list_size = ListVector::GetListSize(map_keys);
	ListVector::SetListSize(result, list_size);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

static unique_ptr<FunctionData> MapEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("The input argument must be a map");
	}
	auto &map = arguments[0]->return_type;

	if (map.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}
	auto &key_type = MapType::KeyType(map);
	auto &value_type = MapType::ValueType(map);

	child_types.push_back(make_pair("k", key_type));
	child_types.push_back(make_pair("v", value_type));

	auto row_type = LogicalType::STRUCT(move(child_types));

	bound_function.return_type = LogicalType::LIST(row_type);
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapEntriesFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_entries", {}, LogicalTypeId::LIST, MapEntriesFunction, MapEntriesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

}; // namespace duckdb
