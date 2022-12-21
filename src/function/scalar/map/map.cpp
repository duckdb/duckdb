#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

MapInvalidReason CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel) {
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	UnifiedVectorFormat map_vdata;

	map.ToUnifiedFormat(count, map_vdata);
	auto &map_validity = map_vdata.validity;

	auto list_data = ListVector::GetData(map);
	auto &keys = MapVector::GetKeys(map);
	UnifiedVectorFormat key_vdata;
	keys.ToUnifiedFormat(count, key_vdata);
	auto &key_validity = key_vdata.validity;

	for (idx_t row = 0; row < count; row++) {
		auto mapped_row = sel.get_index(row);
		auto row_idx = map_vdata.sel->get_index(mapped_row);
		// map is allowed to be NULL
		if (!map_validity.RowIsValid(row_idx)) {
			continue;
		}
		row_idx = key_vdata.sel->get_index(row);
		value_set_t unique_keys;
		for (idx_t i = 0; i < list_data[row_idx].length; i++) {
			auto index = list_data[row_idx].offset + i;
			index = key_vdata.sel->get_index(index);
			if (!key_validity.RowIsValid(index)) {
				return MapInvalidReason::NULL_KEY;
			}
			auto value = keys.GetValue(index);
			auto result = unique_keys.insert(value);
			if (!result.second) {
				return MapInvalidReason::DUPLICATE_KEY;
			}
		}
	}
	return MapInvalidReason::VALID;
}

void MapConversionVerify(Vector &vector, idx_t count) {
	auto valid_check = CheckMapValidity(vector, count);
	switch (valid_check) {
	case MapInvalidReason::VALID:
		break;
	case MapInvalidReason::DUPLICATE_KEY: {
		throw InvalidInputException("Map keys have to be unique");
	}
	case MapInvalidReason::NULL_KEY: {
		throw InvalidInputException("Map keys can not be NULL");
	}
	case MapInvalidReason::NULL_KEY_LIST: {
		throw InvalidInputException("The list of map keys is not allowed to be NULL");
	}
	default: {
		throw InternalException("MapInvalidReason not implemented");
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

	auto &key_vector = MapVector::GetKeys(result);
	auto &value_vector = MapVector::GetValues(result);
	auto list_data = ListVector::GetData(result);

	if (args.data.empty()) {
		ListVector::SetListSize(result, 0);
		list_data->offset = 0;
		list_data->length = 0;
		result.Verify(args.size());
		return;
	}

	auto args_data = ListVector::GetData(args.data[0]);
	auto key_count = ListVector::GetListSize(args.data[0]);
	auto value_count = ListVector::GetListSize(args.data[1]);
	if (key_count != value_count) {
		throw InvalidInputException(
		    "Error in MAP creation: key list has a different size from value list (%lld keys, %lld values)", key_count,
		    value_count);
	}
	ListVector::Reserve(result, key_count);
	ListVector::SetListSize(result, key_count);

	for (idx_t i = 0; i < args.size(); i++) {
		list_data[i] = args_data[i];
	}

	key_vector.Reference(ListVector::GetEntry(args.data[0]));
	value_vector.Reference(ListVector::GetEntry(args.data[1]));
	MapConversionVerify(result, args.size());
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

	bound_function.return_type =
	    LogicalType::MAP(ListType::GetChildType(child_types[0].second), ListType::GetChildType(child_types[1].second));

	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalTypeId::MAP, MapFunction, MapBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction(fun);
}

} // namespace duckdb
