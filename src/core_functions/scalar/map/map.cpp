#include "duckdb/core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

// Example:
// source: [1,2,3], expansion_factor: 4
// target (result): [1,2,3,1,2,3,1,2,3,1,2,3]
static void CreateExpandedVector(const Vector &source, Vector &target, idx_t expansion_factor) {
	idx_t count = ListVector::GetListSize(source);
	auto &entry = ListVector::GetEntry(source);

	idx_t target_idx = 0;
	for (idx_t copy = 0; copy < expansion_factor; copy++) {
		for (idx_t key_idx = 0; key_idx < count; key_idx++) {
			target.SetValue(target_idx, entry.GetValue(key_idx));
			target_idx++;
		}
	}
	D_ASSERT(target_idx == count * expansion_factor);
}

static void AlignVectorToReference(const Vector &original, const Vector &reference, idx_t tuple_count, Vector &result) {
	auto original_length = ListVector::GetListSize(original);
	auto new_length = ListVector::GetListSize(reference);

	Vector expanded_const(ListType::GetChildType(original.GetType()), new_length);

	auto expansion_factor = new_length / original_length;
	if (expansion_factor != tuple_count) {
		throw InvalidInputException("Error in MAP creation: key list and value list do not align. i.e. different "
		                            "size or incompatible structure");
	}
	CreateExpandedVector(original, expanded_const, expansion_factor);
	result.Reference(expanded_const);
}

static bool ListEntriesEqual(Vector &keys, Vector &values, idx_t count) {
	auto key_count = ListVector::GetListSize(keys);
	auto value_count = ListVector::GetListSize(values);
	bool same_vector_type = keys.GetVectorType() == values.GetVectorType();

	D_ASSERT(keys.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(values.GetType().id() == LogicalTypeId::LIST);

	UnifiedVectorFormat keys_data;
	UnifiedVectorFormat values_data;

	keys.ToUnifiedFormat(count, keys_data);
	values.ToUnifiedFormat(count, values_data);

	auto keys_entries = UnifiedVectorFormat::GetData<list_entry_t>(keys_data);
	auto values_entries = UnifiedVectorFormat::GetData<list_entry_t>(values_data);

	if (same_vector_type) {
		const auto key_data = keys_data.data;
		const auto value_data = values_data.data;

		if (keys.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			D_ASSERT(values.GetVectorType() == VectorType::CONSTANT_VECTOR);
			// Only need to compare one entry in this case
			return memcmp(key_data, value_data, sizeof(list_entry_t)) == 0;
		}

		// Fast path if the vector types are equal, can just check if the entries are the same
		if (key_count != value_count) {
			return false;
		}
		return memcmp(key_data, value_data, count * sizeof(list_entry_t)) == 0;
	}

	// Compare the list_entries one by one
	for (idx_t i = 0; i < count; i++) {
		auto keys_idx = keys_data.sel->get_index(i);
		auto values_idx = values_data.sel->get_index(i);

		if (keys_entries[keys_idx] != values_entries[values_idx]) {
			return false;
		}
	}
	return true;
}

static void MapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::MAP);

	auto &key_vector = MapVector::GetKeys(result);
	auto &value_vector = MapVector::GetValues(result);
	auto result_data = ListVector::GetData(result);

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	if (args.data.empty()) {
		ListVector::SetListSize(result, 0);
		result_data->offset = 0;
		result_data->length = 0;
		result.Verify(args.size());
		return;
	}

	bool keys_are_const = args.data[0].GetVectorType() == VectorType::CONSTANT_VECTOR;
	bool values_are_const = args.data[1].GetVectorType() == VectorType::CONSTANT_VECTOR;
	if (!keys_are_const || !values_are_const) {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}

	auto key_count = ListVector::GetListSize(args.data[0]);
	auto value_count = ListVector::GetListSize(args.data[1]);
	auto key_data = ListVector::GetData(args.data[0]);
	auto value_data = ListVector::GetData(args.data[1]);
	auto src_data = key_data;

	if (keys_are_const && !values_are_const) {
		AlignVectorToReference(args.data[0], args.data[1], args.size(), key_vector);
		src_data = value_data;
	} else if (values_are_const && !keys_are_const) {
		AlignVectorToReference(args.data[1], args.data[0], args.size(), value_vector);
	} else {
		if (!ListEntriesEqual(args.data[0], args.data[1], args.size())) {
			throw InvalidInputException("Error in MAP creation: key list and value list do not align. i.e. different "
			                            "size or incompatible structure");
		}
	}

	ListVector::SetListSize(result, MaxValue(key_count, value_count));

	result_data = ListVector::GetData(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i] = src_data[i];
	}

	// check whether one of the vectors has already been referenced to an expanded vector in the case of const/non-const
	// combination. If not, then referencing is still necessary
	if (!(keys_are_const && !values_are_const)) {
		key_vector.Reference(ListVector::GetEntry(args.data[0]));
	}
	if (!(values_are_const && !keys_are_const)) {
		value_vector.Reference(ListVector::GetEntry(args.data[1]));
	}

	MapVector::MapConversionVerify(result, args.size());
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

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::MAP, MapFunction, MapBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
