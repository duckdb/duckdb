#include "duckdb/core_functions/scalar/list_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

void ListFlattenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);

	Vector &input = args.data[0];
	if (input.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(input);
		return;
	}

	idx_t count = args.size();

	// Prepare the result vector
	result.SetVectorType(VectorType::FLAT_VECTOR);
	// This holds the new offsets and lengths
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	// The outermost list in each row
	UnifiedVectorFormat row_data;
	input.ToUnifiedFormat(count, row_data);
	auto row_entries = UnifiedVectorFormat::GetData<list_entry_t>(row_data);

	// The list elements in each row: [HERE, ...]
	auto &row_lists = ListVector::GetEntry(input);
	UnifiedVectorFormat row_lists_data;
	idx_t total_row_lists = ListVector::GetListSize(input);
	row_lists.ToUnifiedFormat(total_row_lists, row_lists_data);
	auto row_lists_entries = UnifiedVectorFormat::GetData<list_entry_t>(row_lists_data);

	if (row_lists.GetType().id() == LogicalTypeId::SQLNULL) {
		for (idx_t row_cnt = 0; row_cnt < count; row_cnt++) {
			auto row_idx = row_data.sel->get_index(row_cnt);
			if (!row_data.validity.RowIsValid(row_idx)) {
				result_validity.SetInvalid(row_cnt);
				continue;
			}
			result_entries[row_cnt].offset = 0;
			result_entries[row_cnt].length = 0;
		}
		if (args.AllConstant()) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		return;
	}

	// The actual elements inside each row list: [[HERE, ...], []]
	// This one becomes the child vector of the result.
	auto &elem_vector = ListVector::GetEntry(row_lists);

	// We'll use this selection vector to slice the elem_vector.
	idx_t child_elem_cnt = ListVector::GetListSize(row_lists);
	SelectionVector sel(child_elem_cnt);
	idx_t sel_idx = 0;

	// HERE, [[]], ...
	for (idx_t row_cnt = 0; row_cnt < count; row_cnt++) {
		auto row_idx = row_data.sel->get_index(row_cnt);

		if (!row_data.validity.RowIsValid(row_idx)) {
			result_validity.SetInvalid(row_cnt);
			continue;
		}

		idx_t list_offset = sel_idx;
		idx_t list_length = 0;

		// [HERE, [...], ...]
		auto row_entry = row_entries[row_idx];
		for (idx_t row_lists_cnt = 0; row_lists_cnt < row_entry.length; row_lists_cnt++) {
			auto row_lists_idx = row_lists_data.sel->get_index(row_entry.offset + row_lists_cnt);

			// Skip invalid lists
			if (!row_lists_data.validity.RowIsValid(row_lists_idx)) {
				continue;
			}

			// [[HERE, ...], [.., ...]]
			auto list_entry = row_lists_entries[row_lists_idx];
			list_length += list_entry.length;

			for (idx_t elem_cnt = 0; elem_cnt < list_entry.length; elem_cnt++) {
				// offset of the element in the elem_vector.
				idx_t offset = list_entry.offset + elem_cnt;
				sel.set_index(sel_idx, offset);
				sel_idx++;
			}
		}

		result_entries[row_cnt].offset = list_offset;
		result_entries[row_cnt].length = list_length;
	}

	ListVector::SetListSize(result, sel_idx);

	auto &result_child_vector = ListVector::GetEntry(result);
	result_child_vector.Slice(elem_vector, sel, sel_idx);
	result_child_vector.Flatten(sel_idx);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListFlattenBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::ARRAY) {
		auto child_type = ArrayType::GetChildType(arguments[0]->return_type);
		if (child_type.id() == LogicalTypeId::ARRAY) {
			child_type = LogicalType::LIST(ArrayType::GetChildType(child_type));
		}
		arguments[0] =
		    BoundCastExpression::AddCastToType(context, std::move(arguments[0]), LogicalType::LIST(child_type));
	} else if (arguments[0]->return_type.id() == LogicalTypeId::LIST) {
		auto child_type = ListType::GetChildType(arguments[0]->return_type);
		if (child_type.id() == LogicalTypeId::ARRAY) {
			child_type = LogicalType::LIST(ArrayType::GetChildType(child_type));
			arguments[0] =
			    BoundCastExpression::AddCastToType(context, std::move(arguments[0]), LogicalType::LIST(child_type));
		}
	}

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
