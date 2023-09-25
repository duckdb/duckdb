#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"

namespace duckdb {

static void ListSelectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);
	Vector &list = args.data[0];
	Vector &selection_list = args.data[1];
	idx_t count = args.size();
	idx_t result_length = ListVector::GetListSize(selection_list);

	list_entry_t *result_data;
	result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_entry = ListVector::GetEntry(result);

	UnifiedVectorFormat selection_lists;
	selection_list.ToUnifiedFormat(count, selection_lists);
	auto selection_lists_data = UnifiedVectorFormat::GetData<list_entry_t>(selection_lists);
	auto &selection_entry = ListVector::GetEntry(selection_list);

	UnifiedVectorFormat input_list;
	list.ToUnifiedFormat(count, input_list);
	auto input_lists_data = UnifiedVectorFormat::GetData<list_entry_t>(input_list);
	auto &input_entry = ListVector::GetEntry(list);
	auto &input_validity = FlatVector::Validity(input_entry);

	ListVector::Reserve(result, input_list.validity.CountValid(count) * result_length);
	ListVector::SetListSize(result, input_list.validity.CountValid(count) * result_length);

	SelectionVector result_selection_vec = SelectionVector(input_list.validity.CountValid(count) * result_length);
	ValidityMask entry_validity_mask = ValidityMask(input_list.validity.CountValid(count) * result_length);
	ValidityMask &result_validity_mask = FlatVector::Validity(result);

	idx_t offset = 0;
	for (idx_t j = 0; j < count; j++) {
		// Get length and offset of selection list for current output row
		auto selection_list_idx = selection_lists.sel->get_index(j);
		idx_t selection_len = 0;
		idx_t selection_offset = 0;
		if (selection_lists.validity.RowIsValid(selection_list_idx)) {
			selection_len = selection_lists_data[selection_list_idx].length;
			selection_offset = selection_lists_data[selection_list_idx].offset;
		} else {
			result_validity_mask.SetInvalid(j);
			continue;
		}
		// Get length and offset of input list for current output row
		auto input_list_idx = input_list.sel->get_index(j);
		idx_t input_length = 0;
		idx_t input_offset = 0;
		if (input_list.validity.RowIsValid(input_list_idx)) {
			input_length = input_lists_data[input_list_idx].length;
			input_offset = input_lists_data[input_list_idx].offset;
		} else {
			result_validity_mask.SetInvalid(j);
			continue;
		}

		// Set all selected values in the result
		for (idx_t child_idx = 0; child_idx < selection_len; child_idx++) {
			if (selection_entry.GetValue(selection_offset + child_idx).IsNull()) {
				throw InvalidInputException("No NULLs are allowed in the selection list.");
			}
			idx_t sel_idx = selection_entry.GetValue(selection_offset + child_idx).GetValue<int>() - 1;
			if (sel_idx < input_length) {
				result_selection_vec.set_index(offset + child_idx, input_offset + sel_idx);
				if (!input_validity.RowIsValid(input_offset + sel_idx)) {
					entry_validity_mask.SetInvalid(offset + child_idx);
				}
			} else {
				result_selection_vec.set_index(offset + child_idx, 0);
				entry_validity_mask.SetInvalid(offset + child_idx);
			}
		}
		result_data[j].length = selection_len;
		result_data[j].offset = offset;
		offset += selection_len;
	}
	result_entry.Slice(input_entry, result_selection_vec, count);
	result_entry.Flatten(offset);
	FlatVector::SetValidity(result_entry, entry_validity_mask);
	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
}

static unique_ptr<FunctionData> ListSelectBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	LogicalType child_type;
	if (arguments[0]->return_type == LogicalTypeId::UNKNOWN || arguments[1]->return_type == LogicalTypeId::UNKNOWN) {
		bound_function.arguments[0] = LogicalTypeId::UNKNOWN;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id() ||
	         LogicalTypeId::SQLNULL == arguments[0]->return_type.id());
	D_ASSERT(LogicalTypeId::LIST == arguments[1]->return_type.id() ||
	         LogicalTypeId::SQLNULL == arguments[1]->return_type.id());

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListSelectFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::LIST(LogicalTypeId::ANY), LogicalType::LIST(LogicalType::BIGINT)},
	                          LogicalType::LIST(LogicalTypeId::ANY), ListSelectFunction, ListSelectBind);
	return fun;
}

void ListSelectFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_select", "array_select"}, GetFunction());
}
} // namespace duckdb
