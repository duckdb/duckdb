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

	// Get the arguments vector
	auto &input_list = args.data[0];
	auto arg_data = FlatVector::GetData<list_entry_t>(input_list);
	auto &entries = ListVector::GetEntry(input_list);

	// Prepare the result vectors
	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = *child_entries[0];
	auto &value_vector = *child_entries[1];
	auto &result_validity = FlatVector::Validity(result);

	// Get the offset+length data for the list(s)
	auto key_data = FlatVector::GetData<list_entry_t>(key_vector);
	auto value_data = FlatVector::GetData<list_entry_t>(value_vector);

	auto &key_validity = FlatVector::Validity(key_vector);
	auto &value_validity = FlatVector::Validity(value_vector);

	auto count = args.size();

	UnifiedVectorFormat input_list_data;
	input_list.ToUnifiedFormat(count, input_list_data);

	// Current offset into the keys/values list
	idx_t offset = 0;

	// Transform to mapped values
	for (idx_t i = 0; i < count; i++) {
		VectorInfo input {entries, arg_data[i]};
		VectorInfo keys {key_vector, key_data[i]};
		VectorInfo values {value_vector, value_data[i]};

		keys.data.offset = offset;
		values.data.offset = offset;
		auto row_valid = MapSingleList(input, keys, values);
		offset += keys.data.length;

		// Check validity
		if (!row_valid || !input_list_data.validity.RowIsValid(i)) {
			key_validity.SetInvalid(i);
			value_validity.SetInvalid(i);
			result_validity.SetInvalid(i);
		}
	}
	MapConversionVerify(result, count);
	result.Verify(count);
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
