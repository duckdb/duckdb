#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

struct VectorInfo {
	Vector &container;
	list_entry_t &data;
};

static void MapStruct(Value &element, VectorInfo &keys, VectorInfo &values) {
	D_ASSERT(element.type().id() == LogicalTypeId::STRUCT);
	D_ASSERT(!element.IsNull());
	auto &key_value = StructValue::GetChildren(element);
	auto &key = key_value[0];
	auto &value = key_value[1];

	if (key.IsNull()) {
		throw InvalidInputException("None of the keys of the map can be NULL");
	}
	// Add to the inner key/value lists of the resulting map
	ListVector::PushBack(keys.container, key);
	ListVector::PushBack(values.container, value);
}

// FIXME: this operation has a time complexity of O(n^2)
void CheckKeyUniqueness(VectorInfo &keys) {
	auto end = keys.data.offset + keys.data.length;
	auto &entries = ListVector::GetEntry(keys.container);
	for (auto lhs = keys.data.offset; lhs < end; lhs++) {
		auto element = entries.GetValue(lhs);
		D_ASSERT(!element.IsNull());
		for (auto rhs = lhs + 1; rhs < end; rhs++) {
			auto other = entries.GetValue(rhs);
			D_ASSERT(!other.IsNull());

			if (element.type() != other.type()) {
				throw InvalidInputException("Not all keys are of the same type!");
			}
			if (element == other) {
				throw InvalidInputException("The given keys aren't unique");
			}
		}
	}
}

static bool MapSingleList(VectorInfo &input, VectorInfo &keys, VectorInfo &values) {
	// Get the length and offset of this list from the argument data
	auto pair_amount = input.data.length;
	auto input_offset = input.data.offset;

	// Loop over the list of structs
	idx_t inserted_values = 0;
	for (idx_t i = 0; i < pair_amount; i++) {
		auto index = i + input_offset;
		// Get the struct using the offset and the index;
		auto element = input.container.GetValue(index);
		if (element.IsNull()) {
			continue;
		}
		MapStruct(element, keys, values);
		inserted_values++;
	}
	// Set the length of the key value lists
	keys.data.length = inserted_values;
	values.data.length = inserted_values;
	return inserted_values != 0;
}

static void MapFromEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
	auto count = args.size();

	// Get the arguments vector
	auto &list = args.data[0];
	auto &list_entry = ListVector::GetEntry(list);

	// Prepare the result vectors
	auto &keys = MapVector::GetKeys(result);
	auto &values = MapVector::GetValues(result);
	auto &result_validity = FlatVector::Validity(result);

	UnifiedVectorFormat keys_data;
	UnifiedVectorFormat values_data;
	// Get the offset+length data for the list(s)
	keys.ToUnifiedFormat(count, keys_data);
	values.ToUnifiedFormat(count, values_data);

	auto &key_validity = FlatVector::Validity(keys);
	auto &value_validity = FlatVector::Validity(values);

	UnifiedVectorFormat list_data;
	list.ToUnifiedFormat(count, list_data);

	// Current offset into the keys/values list
	idx_t offset = 0;

	// Transform to mapped values
	for (idx_t i = 0; i < count; i++) {
		idx_t row = list_data.sel->get_index(i);
		auto *list_entries_array = (list_entry_t *)list_data.data;
		auto *keys_entries_array = (list_entry_t *)keys_data.data;
		auto *values_entries_array = (list_entry_t *)values_data.data;

		VectorInfo list_info {list_entry, list_entries_array[row]};
		VectorInfo keys_info {keys, keys_entries_array[i]};
		VectorInfo values_info {values, values_entries_array[i]};

		keys_info.data.offset = offset;
		values_info.data.offset = offset;
		auto row_valid = MapSingleList(list_info, keys_info, values_info);
		offset += keys_info.data.length;

		// Check validity
		if (!row_valid || !list_data.validity.RowIsValid(row)) {
			key_validity.SetInvalid(i);
			value_validity.SetInvalid(i);
			result_validity.SetInvalid(i);
		}
	}
	MapConversionVerify(result, count);
	result.Verify(count);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> MapFromEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("The input argument must be a list of structs.");
	}
	auto &list = arguments[0]->return_type;

	if (list.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (list.id() != LogicalTypeId::LIST) {
		throw InvalidInputException("The provided argument is not a list of structs");
	}
	auto &elem_type = ListType::GetChildType(list);
	if (elem_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("The elements of the list must be structs");
	}
	auto &children = StructType::GetChildTypes(elem_type);
	if (children.size() != 2) {
		throw InvalidInputException("The provided struct type should only contain 2 fields, a key and a value");
	}
	child_types.push_back(make_pair("key", LogicalType::LIST(children[0].second)));
	child_types.push_back(make_pair("value", LogicalType::LIST(children[1].second)));

	//! this is more for completeness reasons
	bound_function.return_type = LogicalType::MAP(move(child_types));
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapFromEntriesFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_from_entries", {}, LogicalTypeId::MAP, MapFromEntriesFunction, MapFromEntriesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
