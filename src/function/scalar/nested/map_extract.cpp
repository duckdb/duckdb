#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {
void FillResult(Value &values, Vector &result) {
	//! First Initialize List Vector
	ListVector::Initialize(result);

	//! Push Values to List Vector
	for (idx_t i = 0; i < values.list_value.size(); i++) {
		ListVector::PushBack(result, values.list_value[i]);
	}

	//! now set the pointer
	auto &entry = ((list_entry_t *)result.GetData())[0];
	entry.length = values.list_value.size();
	entry.offset = 0;
}

static void MapExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	D_ASSERT(args.data[0].GetType().id() == LogicalTypeId::MAP);

	auto &map = args.data[0];
	auto &key = args.data[1];

	auto &children = StructVector::GetEntries(map);
	auto key_value = key.GetValue(0);
	if (children[0].second->GetType().child_types()[0].second != LogicalTypeId::SQLNULL) {
		key_value = key_value.CastAs(children[0].second->GetType().child_types()[0].second);
	}
	auto offsets = ListVector::Search(*children[0].second, key_value);

	auto values = ListVector::GetValuesFromOffsets(*children[1].second, offsets);
	FillResult(values, result);
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
	auto key_type = arguments[0]->return_type.child_types()[0].second.child_types()[0].second;
	auto value_type = arguments[0]->return_type.child_types()[1].second.child_types()[0].second;

	//! Here we have to construct the List Type that will be returned
	child_list_t<LogicalType> children;
	children.push_back(std::make_pair("", value_type));

	bound_function.return_type = LogicalType(LogicalTypeId::LIST, move(children));
	return make_unique<VariableReturnBindData>(value_type);
}

void MapExtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun("map_extract", {LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunction, false,
	                   MapExtractBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
	fun.name = "element_at";
	set.AddFunction(fun);
}

} // namespace duckdb
