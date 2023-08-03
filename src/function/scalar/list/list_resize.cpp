#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

void ListResizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetType().id() == LogicalTypeId::UBIGINT);
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		FlatVector::SetNull(result, 0, true);
		return;
	}
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &lists = args.data[0];
	auto &child = ListVector::GetEntry(args.data[0]);
	auto &new_sizes = args.data[1];

	UnifiedVectorFormat list_data;
	lists.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	UnifiedVectorFormat new_size_data;
	new_sizes.ToUnifiedFormat(count, new_size_data);
	auto new_size_entries = UnifiedVectorFormat::GetData<int64_t>(new_size_data);

	UnifiedVectorFormat child_data;
	child.ToUnifiedFormat(count, child_data);

	// Find the new size of the result child vector
	idx_t new_child_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto index = new_size_data.sel->get_index(i);
		if (new_size_data.validity.RowIsValid(index)) {
			new_child_size += new_size_entries[index];
		}
	}

	// Create the default vector if it exists
	UnifiedVectorFormat default_data;
	optional_ptr<Vector> default_vector;
	if (args.ColumnCount() == 3) {
		default_vector = &args.data[2];
		default_vector->ToUnifiedFormat(count, default_data);
		default_vector->SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	ListVector::Reserve(result, new_child_size);
	ListVector::SetListSize(result, new_child_size);

	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_child = ListVector::GetEntry(result);

	// for each lists in the args
	idx_t result_child_offset = 0;
	for (idx_t args_index = 0; args_index < count; args_index++) {
		auto l_index = list_data.sel->get_index(args_index);
		auto new_index = new_size_data.sel->get_index(args_index);

		// set null if lists is null
		if (!list_data.validity.RowIsValid(l_index)) {
			FlatVector::SetNull(result, args_index, true);
			continue;
		}

		idx_t new_size_entry = 0;
		if (new_size_data.validity.RowIsValid(new_index)) {
			new_size_entry = new_size_entries[new_index];
		}

		// find the smallest size between lists and new_sizes
		auto values_to_copy = MinValue<idx_t>(list_entries[l_index].length, new_size_entry);

		// set the result entry
		result_entries[args_index].offset = result_child_offset;
		result_entries[args_index].length = new_size_entry;

		// copy the values from the child vector
		VectorOperations::Copy(child, result_child, list_entries[l_index].offset + values_to_copy,
		                       list_entries[l_index].offset, result_child_offset);
		result_child_offset += values_to_copy;

		// set default value if it exists
		idx_t def_index = 0;
		if (args.ColumnCount() == 3) {
			def_index = default_data.sel->get_index(args_index);
		}

		// if the new size is larger than the old size, fill in the default value
		if (values_to_copy < new_size_entry) {
			if (default_vector && default_data.validity.RowIsValid(def_index)) {
				VectorOperations::Copy(*default_vector, result_child, new_size_entry - values_to_copy, def_index,
				                       result_child_offset);
				result_child_offset += new_size_entry - values_to_copy;
			} else {
				for (idx_t j = values_to_copy; j < new_size_entry; j++) {
					FlatVector::SetNull(result_child, result_child_offset, true);
					result_child_offset++;
				}
			}
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ListResizeBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2 || arguments.size() == 3);
	bound_function.arguments[1] = LogicalType::UBIGINT;

	// first argument is constant NULL
	if (arguments[0]->return_type == LogicalType::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	// prepared statements
	if (arguments[0]->return_type == LogicalType::UNKNOWN) {
		bound_function.return_type = arguments[0]->return_type;
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	// default type does not match list type
	if (bound_function.arguments.size() == 3 &&
	    ListType::GetChildType(arguments[0]->return_type) != arguments[2]->return_type &&
	    arguments[2]->return_type != LogicalTypeId::SQLNULL) {
		bound_function.arguments[2] = ListType::GetChildType(arguments[0]->return_type);
	}

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void ListResizeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunction sfun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY},
	                    LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	sfun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunction dfun({LogicalType::LIST(LogicalTypeId::ANY), LogicalTypeId::ANY, LogicalTypeId::ANY},
	                    LogicalType::LIST(LogicalTypeId::ANY), ListResizeFunction, ListResizeBind);
	dfun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ScalarFunctionSet list_resize("list_resize");
	list_resize.AddFunction(sfun);
	list_resize.AddFunction(dfun);
	set.AddFunction(list_resize);

	ScalarFunctionSet array_resize("array_resize");
	array_resize.AddFunction(sfun);
	array_resize.AddFunction(dfun);
	set.AddFunction(array_resize);
}

} // namespace duckdb
