#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

static void MapFromEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	// ! Otherwise if its not a constant vector, this breaks the optimizer
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	// Get the arguments vector
	auto &array = args.data[0];
	auto arg_data = FlatVector::GetData<list_entry_t>(array);
	auto &entries = ListVector::GetEntry(array);

	// Prepare the result vectors
	auto &child_entries = StructVector::GetEntries(result);
	D_ASSERT(child_entries.size() == 2);
	auto &key_vector = child_entries[0];
	auto &value_vector = child_entries[1];

	//Get the offset+length data for the list(s)
	auto key_data = FlatVector::GetData<list_entry_t>(*key_vector);
	auto value_data = FlatVector::GetData<list_entry_t>(*value_vector);

	// Transform to mapped values
	for (idx_t i = 0; i < args.size(); i++) {
		//Get the length and offset of this list from the argument data
		auto pair_amount = arg_data[i].length;
		auto offset = arg_data[i].offset;

		//Set the offset within the key/value list to mark where this row starts
		key_data[i].offset = offset;
		value_data[i].offset = offset;
		//Loop over the list of structs
		for (idx_t col_idx = 0; col_idx < pair_amount; col_idx++) {
			//Get the struct using the offset and the index;
			auto element = entries.GetValue(offset + col_idx);
			D_ASSERT(element.type().id() == LogicalTypeId::STRUCT);
			if (element.IsNull()) {
				throw BinderException("The list of structs contains a NULL");
			}
			auto &key_value = StructValue::GetChildren(element);

			//Add to the inner key/value lists of the resulting map 
			ListVector::PushBack(*key_vector, key_value[0]);
			ListVector::PushBack(*value_vector, key_value[1]);
		}
		//Set the length of the key value lists
		key_data[i].length = pair_amount;
		value_data[i].length = pair_amount;
	}

	result.Verify(args.size());
}

static unique_ptr<FunctionData> MapFromEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("The input argument must be a list of structs.");
	}
	auto &list = arguments[0]->return_type;
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
	ScalarFunction fun("map_from_entries", {}, LogicalTypeId::MAP, MapFromEntriesFunction, false, MapFromEntriesBind);
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
