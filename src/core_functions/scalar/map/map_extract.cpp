#include "duckdb/core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

struct MapKeyArgFunctor {
	// MAP is a LIST(STRUCT(K,V))
	// meaning the MAP itself is a List, but the child vector that we're interested in (the keys)
	// are a level deeper than the initial child vector

	static Vector &GetList(Vector &map) {
		return map;
	}
	static idx_t GetListSize(Vector &map) {
		return ListVector::GetListSize(map);
	}
	static Vector &GetEntry(Vector &map) {
		return MapVector::GetKeys(map);
	}
};

void FillResult(Vector &map, Vector &offsets, Vector &result, idx_t count) {
	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);

	UnifiedVectorFormat offset_data;
	offsets.ToUnifiedFormat(count, offset_data);

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto entry_count = ListVector::GetListSize(map);
	auto &values_entries = MapVector::GetValues(map);
	UnifiedVectorFormat values_entry_data;
	// Note: this vector can have a different size than the map
	values_entries.ToUnifiedFormat(entry_count, values_entry_data);

	for (idx_t row = 0; row < count; row++) {
		idx_t offset_idx = offset_data.sel->get_index(row);
		auto offset = UnifiedVectorFormat::GetData<int32_t>(offset_data)[offset_idx];

		// Get the current size of the list, for the offset
		idx_t current_offset = ListVector::GetListSize(result);
		if (!offset_data.validity.RowIsValid(offset_idx) || !offset) {
			// Set the entry data for this result row
			auto &entry = result_data[row];
			entry.length = 0;
			entry.offset = current_offset;
			continue;
		}
		// All list indices start at 1, reduce by 1 to get the actual index
		offset--;

		// Get the 'values' list entry corresponding to the offset
		idx_t value_index = map_data.sel->get_index(row);
		auto &value_list_entry = UnifiedVectorFormat::GetData<list_entry_t>(map_data)[value_index];

		// Add the values to the result
		idx_t list_offset = value_list_entry.offset + UnsafeNumericCast<idx_t>(offset);
		// All keys are unique, only one will ever match
		idx_t length = 1;
		ListVector::Append(result, values_entries, length + list_offset, list_offset);

		// Set the entry data for this result row
		auto &entry = result_data[row];
		entry.length = length;
		entry.offset = current_offset;
	}
}

static bool ArgumentIsConstantNull(Vector &argument) {
	return argument.GetType().id() == LogicalTypeId::SQLNULL;
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &map = args.data[0];
	auto &key = args.data[1];

	idx_t tuple_count = args.size();
	// Optimization: because keys are not allowed to be NULL, we can early-out
	if (ArgumentIsConstantNull(map) || ArgumentIsConstantNull(key)) {
		//! We don't need to look through the map if the 'key' to look for is NULL
		ListVector::SetListSize(result, 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(result);
		list_data->offset = 0;
		list_data->length = 0;
		result.Verify(tuple_count);
		return;
	}
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);

	UnifiedVectorFormat map_data;

	// Create the chunk we'll feed to ListPosition
	DataChunk list_position_chunk;
	vector<LogicalType> chunk_types;
	chunk_types.reserve(2);
	chunk_types.push_back(map.GetType());
	chunk_types.push_back(key.GetType());
	list_position_chunk.InitializeEmpty(chunk_types.begin(), chunk_types.end());

	// Populate it with the map keys list and the key vector
	list_position_chunk.data[0].Reference(map);
	list_position_chunk.data[1].Reference(key);
	list_position_chunk.SetCardinality(tuple_count);

	Vector position_vector(LogicalType::LIST(LogicalType::INTEGER), tuple_count);
	// We can pass around state as it's not used by ListPositionFunction anyways
	ListContainsOrPosition<int32_t, PositionFunctor, MapKeyArgFunctor>(list_position_chunk, position_vector);

	FillResult(map, position_vector, result, tuple_count);

	if (tuple_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(tuple_count);
}

static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("MAP_EXTRACT must have exactly two arguments");
	}

	auto &map_type = arguments[0]->return_type;
	auto &input_type = arguments[1]->return_type;

	if (map_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalType::LIST(LogicalTypeId::SQLNULL);
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	if (map_type.id() != LogicalTypeId::MAP) {
		throw BinderException("MAP_EXTRACT can only operate on MAPs");
	}
	auto &value_type = MapType::ValueType(map_type);

	//! Here we have to construct the List Type that will be returned
	bound_function.return_type = LogicalType::LIST(value_type);
	auto key_type = MapType::KeyType(map_type);
	if (key_type.id() != LogicalTypeId::SQLNULL && input_type.id() != LogicalTypeId::SQLNULL) {
		bound_function.arguments[1] = MapType::KeyType(map_type);
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapExtractFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunction, MapExtractBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
