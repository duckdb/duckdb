#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void ListFlattenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Vector &input = args.data[0];
	D_ASSERT(input.GetType().id() == LogicalTypeId::LIST);
	auto &child_vector = ListVector::GetEntry(input);

	idx_t count = args.size();
	VectorData list_data;
	input.Orrify(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	auto child_size = ListVector::GetListSize(input);
	VectorData child_data;
	child_vector.Orrify(child_size, child_data);
	auto child_entries = (list_entry_t *)child_data.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	ListVector::ReferenceEntry(result, child_vector);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto list_entry = list_entries[list_index];

		idx_t length = 0;
		// Find last valid child list entry to get length
		for (idx_t j = list_entry.length - 1; j != (idx_t)-1; j--) {
			auto child_list_index = child_data.sel->get_index(list_entry.offset + j);
			if (child_data.validity.RowIsValid(child_list_index)) {
				length = child_entries[child_list_index].offset + child_entries[child_list_index].length - offset;
				break;
			}
		}
		if (length == 0) {
			result_validity.SetInvalid(i);
		} else {
			result_entries[i].offset = offset;
			result_entries[i].length = length;
			offset += length;
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListFlattenBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &input_type = arguments[0]->return_type;

	D_ASSERT(input_type.id() == LogicalTypeId::LIST);
	// D_ASSERT seems not catching if input_type.id() == SQLNULL
	if (input_type.id() != LogicalTypeId::LIST) {
		throw BinderException("FLATTEN can only operate on LIST(LIST)");
	}
	bound_function.arguments[0] = input_type;

	auto child_type = ListType::GetChildType(input_type);
	if (child_type.id() != LogicalTypeId::LIST) {
		throw BinderException("FLATTEN can only operate on LIST(LIST)");
	}
	bound_function.return_type = child_type;
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListFlattenStats(ClientContext &context, BoundFunctionExpression &expr,
                                                   FunctionData *bind_data,
                                                   vector<unique_ptr<BaseStatistics>> &child_stats) {
	if (!child_stats[0]) {
		return nullptr;
	}
	auto &list_stats = (ListStatistics &)*child_stats[0];
	if (!list_stats.child_stats) {
		return nullptr;
	}
	auto child_copy = list_stats.child_stats->Copy();
	return child_copy;
}

void ListFlattenFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction fun({LogicalType::LIST(LogicalType::LIST(LogicalType::ANY))}, LogicalType::LIST(LogicalType::ANY),
	                   ListFlattenFunction, false, ListFlattenBind, nullptr, ListFlattenStats);
	set.AddFunction({"flatten"}, fun);
}

} // namespace duckdb