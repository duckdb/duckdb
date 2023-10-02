#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void ListFlattenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	Vector &input = args.data[0];
	if (input.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(input);
		return;
	}

	idx_t count = args.size();

	UnifiedVectorFormat list_data;
	input.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);
	auto &child_vector = ListVector::GetEntry(input);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (child_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		for (idx_t i = 0; i < count; i++) {
			auto list_index = list_data.sel->get_index(i);
			if (!list_data.validity.RowIsValid(list_index)) {
				result_validity.SetInvalid(i);
				continue;
			}
			result_entries[i].offset = 0;
			result_entries[i].length = 0;
		}
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return;
	}

	auto child_size = ListVector::GetListSize(input);
	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(child_size, child_data);
	auto child_entries = UnifiedVectorFormat::GetData<list_entry_t>(child_data);
	auto &data_vector = ListVector::GetEntry(child_vector);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto list_entry = list_entries[list_index];

		idx_t source_offset = 0;
		// Find first valid child list entry to get offset
		for (idx_t j = 0; j < list_entry.length; j++) {
			auto child_list_index = child_data.sel->get_index(list_entry.offset + j);
			if (child_data.validity.RowIsValid(child_list_index)) {
				source_offset = child_entries[child_list_index].offset;
				break;
			}
		}

		idx_t length = 0;
		// Find last valid child list entry to get length
		for (idx_t j = list_entry.length - 1; j != (idx_t)-1; j--) {
			auto child_list_index = child_data.sel->get_index(list_entry.offset + j);
			if (child_data.validity.RowIsValid(child_list_index)) {
				auto child_entry = child_entries[child_list_index];
				length = child_entry.offset + child_entry.length - source_offset;
				break;
			}
		}
		ListVector::Append(result, data_vector, source_offset + length, source_offset);

		result_entries[i].offset = offset;
		result_entries[i].length = length;
		offset += length;
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListFlattenBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	auto &input_type = arguments[0]->return_type;
	bound_function.arguments[0] = input_type;
	if (input_type.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalType(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}
	D_ASSERT(input_type.id() == LogicalTypeId::LIST);

	auto child_type = ListType::GetChildType(input_type);
	if (child_type.id() == LogicalType::SQLNULL) {
		bound_function.return_type = input_type;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}
	if (child_type.id() == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalType(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}
	D_ASSERT(child_type.id() == LogicalTypeId::LIST);

	bound_function.return_type = child_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListFlattenStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &list_child_stats = ListStats::GetChildStats(child_stats[0]);
	auto child_copy = list_child_stats.Copy();
	child_copy.Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	return child_copy.ToUnique();
}

ScalarFunction ListFlattenFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::LIST(LogicalType::ANY))}, LogicalType::LIST(LogicalType::ANY),
	                      ListFlattenFunction, ListFlattenBind, nullptr, ListFlattenStats);
}

} // namespace duckdb
