#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

#include <iostream>

namespace duckdb {

struct StandardCopyValue {
	template <class T>
	static void Operation(T *result_child_data, T *child_entries, Vector &,
	                      Vector &, idx_t i, idx_t result_child_offset) {
		result_child_data[result_child_offset] = child_entries[i];
	}
};

struct StringCopyValue {
	template <class T>
	static void Operation(T *result_child_data, T *child_entries, Vector &,
	                      Vector &result_child, idx_t i, idx_t result_child_offset) {
		result_child_data[result_child_offset] = StringVector::AddString(result_child, child_entries[i]);
	}
};

struct NestedCopyValue {
	template <class T>
	static void Operation(T *, T *, Vector &child,
	                      Vector &result_child, idx_t i, idx_t result_child_offset) {
		result_child.SetValue(result_child_offset, child.GetValue(i));
	}
};

template <class T, class COPY_FUNCTION>
static void TemplatedListResizeFunction(DataChunk &args, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &list = args.data[0];
	auto &child = ListVector::GetEntry(args.data[0]);
	auto &new_size = args.data[1];

	UnifiedVectorFormat list_data;
	UnifiedVectorFormat new_size_data;
	UnifiedVectorFormat child_data;
	list.ToUnifiedFormat(count, list_data);
	new_size.ToUnifiedFormat(count, new_size_data);
	child.ToUnifiedFormat(count, child_data);
	auto list_entries = (list_entry_t*)list_data.data;
	auto new_size_entries = (int64_t *)new_size_data.data;
	auto child_entries = (T *)child_data.data;


	// Find the new size of the result child vector
	auto new_child_size = 0;
	for (idx_t i = 0; i < count; i++) {
		auto index = list_data.sel->get_index(i);
		new_child_size += new_size_entries[index];
	}

	// Create the default vector if it exists
	bool has_default = false;
	T*default_entries = nullptr;
    UnifiedVectorFormat default_data;
    Vector &default_vector = args.data[0];
	if (args.ColumnCount() == 3) {
		default_vector.ReferenceAndSetType(args.data[2]);
		D_ASSERT(child.GetType() == args.data[2].GetType() || args.data[2].GetType() == LogicalTypeId::SQLNULL);
		default_vector.ToUnifiedFormat(count, default_data);
		default_entries = (T *)default_data.data;
	}

	ListVector::Reserve(result, new_child_size);
	ListVector::SetListSize(result, new_child_size);

	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_child = ListVector::GetEntry(result);
	auto result_child_data = FlatVector::GetData<T>(result_child);

	// for each list in the args
	idx_t result_child_offset = 0;
	for (idx_t args_index = 0; args_index < count; args_index++) {
		auto l_index = list_data.sel->get_index(args_index);
		auto new_index = new_size_data.sel->get_index(args_index);

		// set null if list is null
		if (!list_data.validity.RowIsValid(l_index)) {
			FlatVector::SetNull(result, args_index, true);
			continue;
		}

		// set default value if it exists
		idx_t def_index = 0;
		if (args.ColumnCount() == 3) {
			def_index = default_data.sel->get_index(args_index);
            if (default_data.validity.RowIsValid(def_index)) {
                has_default = true;
            }
		}

		// find the smallest size between list and new_size
		auto values_to_copy = MinValue<idx_t>(list_entries[l_index].length, new_size_entries[new_index]);

		// set the result entry
		result_entries[args_index].offset = result_child_offset;
		result_entries[args_index].length = new_size_entries[new_index];

		// copy the values from the child vector
		for (idx_t list_index = 0; list_index < values_to_copy; list_index++) {
			if (!child_data.validity.RowIsValid(list_entries[l_index].offset + list_index)) {
				FlatVector::SetNull(result_child, result_child_offset, true);
			} else {
				COPY_FUNCTION::template Operation<T>(result_child_data, child_entries, child, result_child, list_entries[l_index].offset + list_index,
				                                     result_child_offset);
			}
			result_child_offset++;
		}

		// if the new size is larger than the old size, fill in the default value
		auto new_size_entry = (idx_t)new_size_entries[new_index];
		for (idx_t j = values_to_copy; j < new_size_entry; j++) {
			if (has_default) {
				COPY_FUNCTION::template Operation<T>(result_child_data, default_entries, default_vector, result_child, def_index,
				                                     result_child_offset);
			} else {
				FlatVector::SetNull(result_child, result_child_offset, true);
			}
			result_child_offset++;
		}

		has_default = false;
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void ListResizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data[1].GetType().id() == LogicalTypeId::BIGINT);
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		FlatVector::Validity(result).SetInvalid(0);
		return;
	}
	auto child_type = ListType::GetChildType(args.data[0].GetType()).InternalType();
	switch (child_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedListResizeFunction<int8_t, StandardCopyValue> (args, result);
		break;
	case PhysicalType::INT16:
		TemplatedListResizeFunction<int16_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::INT32:
		TemplatedListResizeFunction<int32_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::INT64:
		TemplatedListResizeFunction<int64_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::INT128:
		TemplatedListResizeFunction<hugeint_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::UINT8:
		TemplatedListResizeFunction<uint8_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::UINT16:
		TemplatedListResizeFunction<uint16_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::UINT32:
		TemplatedListResizeFunction<uint32_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::UINT64:
		TemplatedListResizeFunction<uint64_t, StandardCopyValue>(args, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedListResizeFunction<float, StandardCopyValue>(args, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedListResizeFunction<double, StandardCopyValue>(args, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedListResizeFunction<string_t, StringCopyValue>(args, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedListResizeFunction<interval_t, NestedCopyValue>(args, result);
		break;
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		TemplatedListResizeFunction<int8_t, NestedCopyValue>(args, result);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for physical type %s",
		                              TypeIdToString(child_type));
	}
}

static unique_ptr<FunctionData> ListResizeBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}


void ListResizeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet resize("list_resize");
	auto fun = ScalarFunction("list_resize", {LogicalTypeId::ANY, LogicalTypeId::BIGINT}, LogicalTypeId::ANY,
	                          ListResizeFunction, ListResizeBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	resize.AddFunction(fun);
	fun.arguments = {LogicalTypeId::ANY, LogicalTypeId::BIGINT, LogicalTypeId::ANY};
	resize.AddFunction(fun);

	set.AddFunction(resize);
}

};
