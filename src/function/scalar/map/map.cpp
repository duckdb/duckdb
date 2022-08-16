#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

// TODO: this doesn't recursively verify maps if maps are nested
MapInvalidReason CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel) {
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	UnifiedVectorFormat map_vdata;
	map.ToUnifiedFormat(count, map_vdata);
	auto &map_validity = map_vdata.validity;

	auto &key_vector = *(StructVector::GetEntries(map)[0]);
	UnifiedVectorFormat key_vdata;
	key_vector.ToUnifiedFormat(count, key_vdata);
	auto key_data = (list_entry_t *)key_vdata.data;
	auto &key_validity = key_vdata.validity;

	auto &key_entries = ListVector::GetEntry(key_vector);
	UnifiedVectorFormat key_entry_vdata;
	key_entries.ToUnifiedFormat(count, key_entry_vdata);
	auto &entry_validity = key_entry_vdata.validity;

	for (idx_t row = 0; row < count; row++) {
		auto mapped_row = sel.get_index(row);
		auto row_idx = map_vdata.sel->get_index(mapped_row);
		// map is allowed to be NULL
		if (!map_validity.RowIsValid(row_idx)) {
			continue;
		}
		row_idx = key_vdata.sel->get_index(row);
		if (!key_validity.RowIsValid(row_idx)) {
			return MapInvalidReason::NULL_KEY_LIST;
		}
		value_set_t unique_keys;
		for (idx_t i = 0; i < key_data[row_idx].length; i++) {
			auto index = key_data[row_idx].offset + i;
			index = key_entry_vdata.sel->get_index(index);
			if (!entry_validity.RowIsValid(index)) {
				return MapInvalidReason::NULL_KEY;
			}
			auto value = key_entries.GetValue(index);
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

	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = *child_entries[0];
	auto &value_vector = *child_entries[1];
	if (args.data.empty()) {
		// no arguments: construct an empty map
		ListVector::SetListSize(key_vector, 0);
		key_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto list_data = ConstantVector::GetData<list_entry_t>(key_vector);
		list_data->offset = 0;
		list_data->length = 0;

		ListVector::SetListSize(value_vector, 0);
		value_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		list_data = ConstantVector::GetData<list_entry_t>(value_vector);
		list_data->offset = 0;
		list_data->length = 0;

		result.Verify(args.size());
		return;
	}

	auto key_count = ListVector::GetListSize(args.data[0]);
	auto value_count = ListVector::GetListSize(args.data[1]);
	if (key_count != value_count) {
		throw InvalidInputException(
		    "Error in MAP creation: key list has a different size from value list (%lld keys, %lld values)", key_count,
		    value_count);
	}

	key_vector.Reference(args.data[0]);
	value_vector.Reference(args.data[1]);
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

	//! this is more for completeness reasons
	bound_function.return_type = LogicalType::MAP(move(child_types));
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
