#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

static void ListSelectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	Vector &list = args.data[0];
	Vector &selection_list = args.data[1];
	idx_t count = args.size();

	ListVector::Reserve(result, ListVector::GetListSize(selection_list));

	bool all_constant = true;
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		auto curr = args.data[i];
		// check if all arguments are constants, if so the result is also a constant vector
		if (curr.GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_constant = false;
		}
	}

	list_entry_t *result_data;
	result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_entry = ListVector::GetEntry(result);

	UnifiedVectorFormat sel_list;
	selection_list.ToUnifiedFormat(count, sel_list);
	auto selection_lists = UnifiedVectorFormat::GetData<list_entry_t>(sel_list);
	auto &sel_entry = ListVector::GetEntry(selection_list);

	UnifiedVectorFormat inp_list;
	list.ToUnifiedFormat(count, inp_list);
	auto input_lists = UnifiedVectorFormat::GetData<list_entry_t>(inp_list);
	auto &input_entry = ListVector::GetEntry(list);

	idx_t offset = 0;
	for (idx_t j = 0; j < count; j++) {
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

		// Set all selected values in the result
		for (idx_t i = 0; i < sel_len; i++) {
			idx_t sel_idx = sel_entry.GetValue(sel_off + i).GetValue<idx_t>();
			Value sel_val = sel_idx < inp_len ? input_entry.GetValue(inp_off + sel_idx) : Value(LogicalType::SQLNULL);
			result_entry.SetValue(offset + i, sel_val);
		}

		result_data[j].length = sel_len;
		result_data[j].offset = offset;
		offset += sel_len;
	}
	ListVector::SetListSize(result, offset);
	result.SetVectorType(all_constant ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
}

static unique_ptr<FunctionData> ListSelectBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
	D_ASSERT(LogicalTypeId::LIST == arguments[1]->return_type.id());

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<BaseStatistics> ListSelectStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;

	auto &left_stats = child_stats[0];
	auto stats = left_stats.ToUnique();

	return stats;
}

ScalarFunction ListSelectFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalTypeId::ANY), LogicalType::LIST(LogicalType::BIGINT)},
	                          LogicalType::LIST(LogicalTypeId::ANY), ListSelectFunction, ListSelectBind, nullptr,
	                          ListSelectStats);
	return fun;
}

void ListSelectFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_select"}, GetFunction());
}
} // namespace duckdb
