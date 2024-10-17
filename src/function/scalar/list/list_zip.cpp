#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

static void ListZipFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	idx_t args_size = args.ColumnCount();
	auto *result_data = FlatVector::GetData<list_entry_t>(result);
	auto &result_struct = ListVector::GetEntry(result);
	auto &struct_entries = StructVector::GetEntries(result_struct);
	bool truncate_flags_set = false;

	// Check flag
	if (args.data.back().GetType().id() == LogicalTypeId::BOOLEAN) {
		truncate_flags_set = true;
		args_size--;
	}

	vector<UnifiedVectorFormat> input_lists;
	input_lists.resize(args.ColumnCount());
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		args.data[i].ToUnifiedFormat(count, input_lists[i]);
	}

	// Handling output row for each input row
	idx_t result_size = 0;
	vector<idx_t> lengths;
	for (idx_t j = 0; j < count; j++) {
		// Is flag for current row set
		bool truncate_to_shortest = false;
		if (truncate_flags_set) {
			auto &flag_vec = input_lists.back();
			idx_t flag_idx = flag_vec.sel->get_index(j);
			if (flag_vec.validity.RowIsValid(flag_idx)) {
				truncate_to_shortest = UnifiedVectorFormat::GetData<bool>(flag_vec)[flag_idx];
			}
		}

		// Calculation of the outgoing list size
		idx_t len = truncate_to_shortest ? NumericLimits<int>::Maximum() : 0;
		for (idx_t i = 0; i < args_size; i++) {
			idx_t curr_size;
			if (args.data[i].GetType() == LogicalType::SQLNULL || ListVector::GetListSize(args.data[i]) == 0) {
				curr_size = 0;
			} else {
				idx_t sel_idx = input_lists[i].sel->get_index(j);
				auto curr_data = UnifiedVectorFormat::GetData<list_entry_t>(input_lists[i]);
				curr_size = input_lists[i].validity.RowIsValid(sel_idx) ? curr_data[sel_idx].length : 0;
			}

			// Dependent on flag using gt or lt
			if (truncate_to_shortest) {
				len = len > curr_size ? curr_size : len;
			} else {
				len = len < curr_size ? curr_size : len;
			}
		}
		lengths.push_back(len);
		result_size += len;
	}

	ListVector::SetListSize(result, result_size);
	ListVector::Reserve(result, result_size);
	vector<SelectionVector> selections;
	vector<ValidityMask> masks;
	for (idx_t i = 0; i < args_size; i++) {
		selections.push_back(SelectionVector(result_size));
		masks.push_back(ValidityMask(result_size));
	}

	idx_t offset = 0;
	for (idx_t j = 0; j < count; j++) {
		idx_t len = lengths[j];
		for (idx_t i = 0; i < args_size; i++) {
			auto &curr = input_lists[i];
			idx_t sel_idx = curr.sel->get_index(j);
			idx_t curr_off = 0;
			idx_t curr_len = 0;

			// Copying values from the given lists
			if (curr.validity.RowIsValid(sel_idx)) {
				auto input_lists_data = UnifiedVectorFormat::GetData<list_entry_t>(curr);
				curr_off = input_lists_data[sel_idx].offset;
				curr_len = input_lists_data[sel_idx].length;
				auto copy_len = len < curr_len ? len : curr_len;
				idx_t entry = offset;
				for (idx_t k = 0; k < copy_len; k++) {
					if (!FlatVector::Validity(ListVector::GetEntry(args.data[i])).RowIsValid(curr_off + k)) {
						masks[i].SetInvalid(entry + k);
					}
					selections[i].set_index(entry + k, curr_off + k);
				}
			}

			// Set NULL values for list that are shorter than the output list
			if (len > curr_len) {
				for (idx_t d = curr_len; d < len; d++) {
					masks[i].SetInvalid(d + offset);
					selections[i].set_index(d + offset, 0);
				}
			}
		}
		result_data[j].length = len;
		result_data[j].offset = offset;
		offset += len;
	}
	for (idx_t child_idx = 0; child_idx < args_size; child_idx++) {
		if (!(args.data[child_idx].GetType() == LogicalType::SQLNULL)) {
			struct_entries[child_idx]->Slice(ListVector::GetEntry(args.data[child_idx]), selections[child_idx],
			                                 result_size);
		}
		struct_entries[child_idx]->Flatten(result_size);
		FlatVector::SetValidity((*struct_entries[child_idx]), masks[child_idx]);
	}
	result.SetVectorType(args.AllConstant() ? VectorType::CONSTANT_VECTOR : VectorType::FLAT_VECTOR);
}

static unique_ptr<FunctionData> ListZipBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> struct_children;

	// The last argument could be a flag to be set if we want a minimal list or a maximal list
	idx_t size = arguments.size();
	if (size == 0) {
		throw BinderException("Provide at least one argument to " + bound_function.name);
	}
	if (arguments[size - 1]->return_type.id() == LogicalTypeId::BOOLEAN) {
		size--;
	}

	case_insensitive_set_t struct_names;
	for (idx_t i = 0; i < size; i++) {
		auto &child = arguments[i];
		switch (child->return_type.id()) {
		case LogicalTypeId::LIST:
		case LogicalTypeId::ARRAY:
			child = BoundCastExpression::AddArrayCastToList(context, std::move(child));
			struct_children.push_back(make_pair(string(), ListType::GetChildType(child->return_type)));
			break;
		case LogicalTypeId::SQLNULL:
			struct_children.push_back(make_pair(string(), LogicalTypeId::SQLNULL));
			break;
		case LogicalTypeId::UNKNOWN:
			throw ParameterNotResolvedException();
		default:
			throw BinderException("Parameter type needs to be List");
		}
	}
	bound_function.return_type = LogicalType::LIST(LogicalType::STRUCT(struct_children));
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListZipFun::GetFunction() {

	auto fun = ScalarFunction({}, LogicalType::LIST(LogicalTypeId::STRUCT), ListZipFunction, ListZipBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING; // Special handling needed?
	return fun;
}

} // namespace duckdb
