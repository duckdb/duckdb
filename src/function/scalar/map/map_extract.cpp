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

static bool ShouldCastKey(const LogicalType &map_key_type, const LogicalType &indexing_type) {
	return map_key_type != LogicalTypeId::SQLNULL && map_key_type != indexing_type;
}

static Value GetKeyToLookFor(Vector &index_vector, idx_t row, const LogicalType &cast_to) {
	auto key_value = index_vector.GetValue(row);
	if (cast_to.id() != LogicalTypeId::SQLNULL) {
		key_value = key_value.CastAs(cast_to);
	}
	return key_value;
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &map = args.data[0];
	auto &key = args.data[1];

	UnifiedVectorFormat offset_data;

	auto &children = StructVector::GetEntries(map);
	auto &map_key_type = ListType::GetChildType(children[0]->GetType());

	bool should_cast = ShouldCastKey(map_key_type, key.GetType());
	auto key_value = key.GetValue(0);

	children[0]->ToUnifiedFormat(args.size(), offset_data);
	for (idx_t row = 0; row < args.size(); row++) {
		idx_t row_index = offset_data.sel->get_index(row);
		auto key_value = GetKeyToLookFor(key, row_index, should_cast ? map_key_type : LogicalType::SQLNULL);
		auto offsets = ListVector::Search(*children[0], key_value, offset_data.sel->get_index(row));
		auto values = ListVector::GetValuesFromOffsets(*children[1], offsets);
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
	auto &child_types = StructType::GetChildTypes(arguments[0]->return_type);
	auto &value_type = ListType::GetChildType(child_types[1].second);

	//! Here we have to construct the List Type that will be returned
	bound_function.return_type = LogicalType::LIST(value_type);
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
