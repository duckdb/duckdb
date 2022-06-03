#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/list_aggregate_function.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

static void CheckForKeyUniqueness(DataChunk &args, ExpressionState &state) {
	// Create a copy of the arguments
	auto types = args.GetTypes();
	if (types.empty() || ListType::GetChildType(types[0]).id() == LogicalType::SQLNULL) {
		return;
	}

	auto arg_data = FlatVector::GetData<list_entry_t>(args.data[0]);

	DataChunk keys;
	keys.Initialize(args.GetTypes());
	args.Copy(keys);

	// Split the copy to separate the keys
	DataChunk remaining_columns;
	keys.Split(remaining_columns, 1);

	Vector unique_result(LogicalType::UBIGINT, args.size());
	ListUniqueFunction(keys, state, unique_result);
	for (idx_t i = 0; i < args.size(); i++) {
		auto keys_length = arg_data[i].length;
		auto unique_keys = FlatVector::GetValue<uint64_t>(unique_result, i);
		if (unique_keys != keys_length) {
			throw InvalidInputException("Map keys have to be unique!");
		}
	}
}

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	//! Otherwise if its not a constant vector, this breaks the optimizer
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	CheckForKeyUniqueness(args, state);

	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = child_entries[0];
	auto &value_vector = child_entries[1];
	if (args.data.empty()) {
		// no arguments: construct an empty map
		ListVector::SetListSize(*key_vector, 0);
		key_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(*key_vector);
		list_data->offset = 0;
		list_data->length = 0;

		ListVector::SetListSize(*value_vector, 0);
		value_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
		list_data = ConstantVector::GetData<list_entry_t>(*value_vector);
		list_data->offset = 0;
		list_data->length = 0;

		result.Verify(args.size());
		return;
	}

	if (ListVector::GetListSize(args.data[0]) != ListVector::GetListSize(args.data[1])) {
		throw Exception("Key list has a different size from Value list");
	}
	key_vector->Reference(args.data[0]);
	value_vector->Reference(args.data[1]);

	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapBind(ClientContext &context, ScalarFunction &bound_function,
                                        vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 2 && !arguments.empty()) {
		throw Exception("We need exactly two lists for a map");
	}
	if (arguments.size() == 2) {
		if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("First argument is not a list");
		}
		if (arguments[1]->return_type.id() != LogicalTypeId::LIST) {
			throw Exception("Second argument is not a list");
		}
		child_types.push_back(make_pair("key", arguments[0]->return_type));
		child_types.push_back(make_pair("value", arguments[1]->return_type));
	}

	if (arguments.empty()) {
		auto empty = LogicalType::LIST(LogicalTypeId::SQLNULL);
		child_types.push_back(make_pair("key", empty));
		child_types.push_back(make_pair("value", empty));
	}

	//! this is more for completeness reasons
	auto key_type = ListType::GetChildType(child_types[0].second);
	bound_function.return_type = LogicalType::MAP(move(child_types));
	if (arguments.empty() || key_type.id() == LogicalTypeId::SQLNULL) {
		return make_unique<VariableReturnBindData>(bound_function.return_type);
	}
	auto aggr_function = HistogramFun::GetHistogramUnorderedMap(key_type);
	bound_function.arguments.push_back(key_type);
	return ListAggregatesBindFunction(context, bound_function, key_type, aggr_function);
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalTypeId::MAP, MapFunction, false, MapBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
