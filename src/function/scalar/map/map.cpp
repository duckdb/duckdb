#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/list_aggregate_function.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

bool KeyListIsEmpty(list_entry_t *data, idx_t rows) {
	for (idx_t i = 0; i < rows; i++) {
		auto size = data[i].length;
		if (size != 0) {
			return false;
		}
	}
	return true;
}

static bool AreKeysNull(Vector &keys, idx_t row_count) {
	auto list_type = keys.GetType();
	D_ASSERT(list_type.id() == LogicalTypeId::LIST);
	auto key_type = ListType::GetChildType(list_type).id();
	auto arg_data = ListVector::GetData(keys);
	auto &entries = ListVector::GetEntry(keys);
	if (key_type == LogicalTypeId::SQLNULL) {
		if (KeyListIsEmpty(arg_data, row_count)) {
			return false;
		}
		// The entire key list is NULL for one (or more) of the rows: (ARRAY[NULL, NULL, NULL])
		return true;
	}

	VectorData list_data;
	keys.Orrify(row_count, list_data);
	auto validity = FlatVector::Validity(entries);
	return (!validity.CheckAllValid(row_count));
}

// TODO replace this with a call to ListUnique
static bool AreKeysUnique(Vector &key_vector, idx_t row_count) {
	D_ASSERT(key_vector.GetType().id() == LogicalTypeId::LIST);
	auto key_type = ListType::GetChildType(key_vector.GetType());
	auto &child_entry = ListVector::GetEntry(key_vector);
	VectorData child_vector_data;
	child_entry.Orrify(ListVector::GetListSize(key_vector), child_vector_data);
	auto child_validity = child_vector_data.validity;

	VectorData vector_data;
	key_vector.Orrify(row_count, vector_data);
	auto key_data = (list_entry_t *)vector_data.data;
	auto key_validity = vector_data.validity;

	for (idx_t row = 0; row < row_count; row++) {
		value_set_t unique_keys;
		auto index = vector_data.sel->get_index(row);

		// Ensure there are no NULL key lists
		if (!key_validity.RowIsValid(index)) {
			throw InvalidInputException("Map keys can not be NULL");
		}

		idx_t start = key_data[index].offset;
		idx_t end = start + key_data[index].length;
		for (idx_t i = start; i < end; i++) {
			auto child_idx = child_vector_data.sel->get_index(i);
			if (!child_validity.RowIsValid(child_idx)) {
				throw InvalidInputException("Map keys can not be NULL");
			}
			auto val = child_entry.GetValue(child_idx);
			auto result = unique_keys.insert(val);
			if (!result.second) {
				// insertion failed because the element already exists
				return false;
			}
		}
	}
	return true;
}

void VerifyKeysUnique(Vector &keys, idx_t count) {
	if (!AreKeysUnique(keys, count)) {
		throw InvalidInputException("Map keys have to be unique");
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

	if (AreKeysNull(args.data[0], args.size())) {
		throw InvalidInputException("Map keys can not be NULL");
	}

	VerifyKeysUnique(args.data[0], args.size());

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
	bound_function.return_type = LogicalType::MAP(move(child_types));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map", {}, LogicalTypeId::MAP, MapFunction, false, MapBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
