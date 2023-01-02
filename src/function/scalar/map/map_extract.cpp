#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
void FillResult(Value &values, Vector &result, idx_t row) {
	//! First Initialize List Vector
	idx_t current_offset = ListVector::GetListSize(result);
	//! Push Values to List Vector
	auto &list_values = ListValue::GetChildren(values);
	for (idx_t i = 0; i < list_values.size(); i++) {
		ListVector::PushBack(result, list_values[i]);
	}

	//! now set the pointer
	auto &entry = ListVector::GetData(result)[row];
	entry.length = list_values.size();
	entry.offset = current_offset;
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	if (args.data[1].GetType().id() == LogicalTypeId::SQLNULL) {
		//! We don't need to look through the map if the 'key' to look for is NULL
		ListVector::SetListSize(result, 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(result);
		list_data->offset = 0;
		list_data->length = 0;
		result.Verify(args.size());
		return;
	}

	auto &map = args.data[0];
	auto &key = args.data[1];

	UnifiedVectorFormat map_data;
	UnifiedVectorFormat key_data;

	auto &map_keys = MapVector::GetKeys(map);
	auto &map_values = MapVector::GetValues(map);

	map.ToUnifiedFormat(args.size(), map_data);
	key.ToUnifiedFormat(args.size(), key_data);

	for (idx_t row = 0; row < args.size(); row++) {
		idx_t row_index = map_data.sel->get_index(row);
		idx_t key_index = key_data.sel->get_index(row);
		auto key_value = key.GetValue(key_index);

		list_entry_t entry = ListVector::GetData(map)[row_index];
		auto offsets = MapVector::Search(map_keys, args.size(), key_value, entry);
		auto values = FlatVector::GetValuesFromOffsets(map_values, offsets);
		FillResult(values, result, row);
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("MAP_EXTRACT must have exactly two arguments");
	}
	if (arguments[0]->return_type.id() != LogicalTypeId::MAP) {
		throw BinderException("MAP_EXTRACT can only operate on MAPs");
	}
	auto &value_type = MapType::ValueType(arguments[0]->return_type);

	//! Here we have to construct the List Type that will be returned
	bound_function.return_type = LogicalType::LIST(value_type);
	auto key_type = MapType::KeyType(arguments[0]->return_type);
	if (key_type.id() != LogicalTypeId::SQLNULL && arguments[1]->return_type.id() != LogicalTypeId::SQLNULL) {
		bound_function.arguments[1] = MapType::KeyType(arguments[0]->return_type);
	}
	return make_unique<VariableReturnBindData>(value_type);
}

void MapExtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun("map_extract", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunction,
	                   MapExtractBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
	fun.name = "element_at";
	set.AddFunction(fun);
}

} // namespace duckdb
