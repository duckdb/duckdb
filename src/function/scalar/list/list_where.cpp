#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

static void ListWhereFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Vector &input_list = args.data[0];
	Vector &selection_list = args.data[1];
	idx_t count = args.size();

	bool all_constant = true;
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		auto curr = args.data[i];
		// check if all arguments are constants, if so the result is also a constant vector
		if (curr.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_constant = false;
		}
	}

	list_entry_t *result_data = FlatVector::GetData<list_entry_t>(result);

	UnifiedVectorFormat sel_list;
	selection_list.ToUnifiedFormat(count, sel_list);
	auto selection_lists = UnifiedVectorFormat::GetData<list_entry_t>(sel_list);
	auto &sel_entry = ListVector::GetEntry(selection_list);

	UnifiedVectorFormat inp_list;
	input_list.ToUnifiedFormat(count, inp_list);
	auto input_lists = UnifiedVectorFormat::GetData<list_entry_t>(inp_list);
	auto &input_entry = ListVector::GetEntry(input_list);

	idx_t offset = 0;
	for (idx_t j = 0; j < count; j++) {
		idx_t len = 0;
		// Get length and offset of selection list for current output row
		auto sel_list_idx = sel_list.sel->get_index(j);
		idx_t sel_len = 0;
		idx_t sel_off = 0;
		if (sel_list.validity.RowIsValid(sel_list_idx)) {
			sel_len = selection_lists[sel_list_idx].length;
			sel_off = selection_lists[sel_list_idx].offset;
		}

		// Get length and offset of input list for current output row
		auto inp_list_idx = inp_list.sel->get_index(j);
		idx_t inp_len = 0;
		idx_t inp_off = 0;
		if (inp_list.validity.RowIsValid(inp_list_idx)) {
			inp_len = input_lists[inp_list_idx].length;
			inp_off = input_lists[inp_list_idx].offset;
		}

		if (inp_len != sel_len) {
			throw InvalidInputException("The first list and the second list must be of the same length.");
		}

		for (idx_t i = 0; i < sel_len; i++) {
			auto sel_val = sel_entry.GetValue(sel_off + i);
			if (sel_val.IsNull()) {
				throw InvalidInputException(
				    "NULL values are not accepted for the second argument of the where function.");
			}
			bool isSelected = sel_val.GetValue<bool>();
			if (isSelected) {
				ListVector::PushBack(result, input_entry.GetValue(inp_off + i));
				len++;
			}
		}
		result_data[j].length = len;
		result_data[j].offset = offset;
		offset += len;
	}
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
}

static unique_ptr<FunctionData> ListWhereBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListWhereStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &left_stats = child_stats[0];
	auto stats = left_stats.ToUnique();
	return stats;
}

ScalarFunction ListWhereFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalTypeId::ANY), LogicalType::LIST(LogicalTypeId::BOOLEAN)},
	                          LogicalType::LIST(LogicalTypeId::ANY), ListWhereFunction, ListWhereBind, nullptr,
	                          ListWhereStats);
	return fun;
}

void ListWhereFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_where"}, GetFunction());
}
} // namespace duckdb
