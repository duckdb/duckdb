#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/list_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

static void ListZipFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();
	auto &lists = StructVector::GetEntries(args.data[0]);
	vector<optional<VectorIterator<list_entry_t>>> input_lists;
	optional<VectorIterator<bool>> truncate_flag;
	for (idx_t i = 0; i < lists.size(); i++) {
		if (i + 1 == lists.size() && lists[i].GetType().id() == LogicalTypeId::BOOLEAN) {
			truncate_flag = lists[i].Values<bool>();
		} else if (lists[i].GetType().id() == LogicalTypeId::LIST) {
			input_lists.emplace_back(lists[i].Values<list_entry_t>());
		} else {
			// SQLNULL — no list iterator possible; treated as always-null input
			input_lists.emplace_back(nullopt);
		}
	}
	idx_t args_size = input_lists.size();

	// Handling output row for each input row
	idx_t result_size = 0;
	vector<idx_t> lengths;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		bool truncate_to_shortest = false;
		if (truncate_flag) {
			auto &truncate_vector = *truncate_flag;
			auto truncate = truncate_vector[row_idx];
			if (truncate.IsValid()) {
				truncate_to_shortest = truncate.GetValue();
			}
		}

		// Calculation of the outgoing list size
		idx_t len = truncate_to_shortest ? NumericLimits<idx_t>::Maximum() : 0;
		for (idx_t i = 0; i < args_size; i++) {
			idx_t curr_size = 0;
			if (input_lists[i].has_value() && ListVector::GetListSize(lists[i]) > 0) {
				auto list_entry = (*input_lists[i])[row_idx];
				if (list_entry.IsValid()) {
					curr_size = list_entry.GetValue().length;
				}
			}

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
	auto &result_struct = ListVector::GetChildMutable(result);
	auto &struct_entries = StructVector::GetEntries(result_struct);
	vector<SelectionVector> selections;
	vector<ValidityMask> masks;
	for (idx_t i = 0; i < args_size; i++) {
		selections.push_back(SelectionVector(result_size));
		masks.push_back(ValidityMask(result_size));
	}

	// Ensure list children are flat so FlatVector::Validity can be used below
	for (idx_t i = 0; i < args_size; i++) {
		if (input_lists[i].has_value() && ListVector::GetListSize(lists[i]) > 0) {
			ListVector::GetChildMutable(lists[i]).Flatten();
		}
	}

	idx_t offset = 0;
	auto result_data = FlatVector::Writer<list_entry_t>(result, count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		idx_t len = lengths[row_idx];
		for (idx_t i = 0; i < args_size; i++) {
			idx_t curr_off = 0;
			idx_t curr_len = 0;

			if (input_lists[i].has_value()) {
				auto list_entry = (*input_lists[i])[row_idx];
				if (list_entry.IsValid()) {
					auto list = list_entry.GetValue();
					curr_off = list.offset;
					curr_len = list.length;
					auto copy_len = len < curr_len ? len : curr_len;
					idx_t entry = offset;
					for (idx_t k = 0; k < copy_len; k++) {
						if (!FlatVector::Validity(ListVector::GetChild(lists[i])).RowIsValid(curr_off + k)) {
							masks[i].SetInvalid(entry + k);
						}
						selections[i].set_index(entry + k, curr_off + k);
					}
				}
			}

			// Set NULL values for entries beyond the valid range of this input list
			if (len > curr_len) {
				for (idx_t d = curr_len; d < len; d++) {
					masks[i].SetInvalid(d + offset);
					selections[i].set_index(d + offset, 0);
				}
			}
		}
		list_entry_t entry;
		entry.length = len;
		entry.offset = offset;
		result_data.WriteValue(entry);
		offset += len;
	}
	for (idx_t child_idx = 0; child_idx < args_size; child_idx++) {
		if (lists[child_idx].GetType() == LogicalType::SQLNULL || ListVector::GetListSize(lists[child_idx]) == 0) {
			struct_entries[child_idx].SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(struct_entries[child_idx], true);
		} else {
			struct_entries[child_idx].Slice(ListVector::GetChild(lists[child_idx]), selections[child_idx], result_size);
		}
		struct_entries[child_idx].Flatten();
		FlatVector::SetValidity((struct_entries[child_idx]), masks[child_idx]);
	}
}

static unique_ptr<FunctionData> ListZipBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &pack = *input.GetArguments()[0];
	auto &arguments = ArgumentPack::GetPackedChildren(pack);
	child_list_t<LogicalType> struct_children;

	// The last argument could be a flag to be set if we want a minimal list or a maximal list
	idx_t size = arguments.size();
	if (size == 0) {
		throw BinderException("Provide at least one argument to " + bound_function.GetName());
	}
	if (arguments[size - 1]->GetReturnType().id() == LogicalTypeId::BOOLEAN) {
		if (--size == 0) {
			throw BinderException("Provide at least one list argument to " + bound_function.GetName());
		}
	}

	identifier_set_t struct_names;
	for (idx_t i = 0; i < size; i++) {
		auto &child = arguments[i];
		switch (child->GetReturnType().id()) {
		case LogicalTypeId::LIST:
		case LogicalTypeId::ARRAY:
			child = BoundCastExpression::AddArrayCastToList(context, std::move(child));
			struct_children.emplace_back(make_pair(string(), ListType::GetChildType(child->GetReturnType())));
			break;
		case LogicalTypeId::SQLNULL:
			struct_children.emplace_back(make_pair(string(), LogicalTypeId::SQLNULL));
			break;
		case LogicalTypeId::UNKNOWN:
			throw ParameterNotResolvedException();
		default:
			throw BinderException("Parameter type needs to be List");
		}
	}
	// the loop above may have cast some of the collected arguments, so the pack has to describe them again
	ArgumentPack::RefreshType(pack);
	bound_function.GetArguments()[0] = pack.GetReturnType();

	bound_function.SetReturnType(LogicalType::LIST(LogicalType::TUPLE(struct_children)));
	return make_uniq<VariableReturnBindData>(bound_function.GetReturnType());
}

ScalarFunction ListZipFun::GetFunction() {
	FunctionSignature signature;
	signature.AddVarPositionalParameter("lists", LogicalType::ANY);
	signature.SetReturnType(LogicalType::LIST(LogicalTypeId::STRUCT));
	auto fun = ScalarFunction("list_zip", std::move(signature), ListZipFunction);
	fun.SetBindCallback(ListZipBind);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
