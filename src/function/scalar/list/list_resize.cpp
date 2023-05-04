#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

#include <iostream>

namespace duckdb {

struct StandardCopyValue {
	template <class T>
	static void Operation(Vector &child, Vector &result_child, idx_t i, idx_t result_child_offset) {
		UnifiedVectorFormat child_data;
		child.ToUnifiedFormat(i, child_data);

		auto child_entries = (T *)child_data.data;
		auto result_child_data = FlatVector::GetData<T>(result_child);

		result_child_data[result_child_offset] = child_entries[i];
	}
};

struct StringCopyValue {
	template <class T>
	static void Operation(Vector &child, Vector &result_child, idx_t i, idx_t result_child_offset) {
		UnifiedVectorFormat child_data;
		child.ToUnifiedFormat(i, child_data);

		auto child_entries = (string_t *)child_data.data;
		auto result_child_data = FlatVector::GetData<string_t>(result_child);

		result_child_data[result_child_offset] = StringVector::AddString(result_child, child_entries[i]);
	}
};

struct NestedCopyValue {
	template <class T>
	static void Operation(Vector &child, Vector &result_child, idx_t i, idx_t result_child_offset) {
		result_child.SetValue(result_child_offset, child.GetValue(i));
	}
};

template <class T, class COPY_FUNCTION>
static void TemplatedListResizeFunction(DataChunk &args, Vector &result) {
	auto &result_validity = FlatVector::Validity(result);
	if (result.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();

	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &list = args.data[0];
	auto &child = ListVector::GetEntry(args.data[0]);
	auto &new_size = args.data[1];

	bool has_default = false;
	T default_value;
	if (args.ColumnCount() == 3) {
		auto &d = args.data[2];
		UnifiedVectorFormat d_data;
		d.ToUnifiedFormat(count, d_data);
		auto d_entries = (T *)d_data.data;
		if (d_data.validity.RowIsValid(0)) {
//			default_value = d_entries[0];
			has_default = true;
		}
	}

	UnifiedVectorFormat list_data;
	UnifiedVectorFormat new_size_data;
	UnifiedVectorFormat child_data;
	list.ToUnifiedFormat(count, list_data);
	new_size.ToUnifiedFormat(count, new_size_data);
	child.ToUnifiedFormat(count, child_data);
	auto list_entries = (list_entry_t*)list_data.data;
	auto new_size_entries = (int64_t *)new_size_data.data;
	auto child_entries = (T *)child_data.data;

	auto new_child_size = 0;

	for (idx_t i = 0; i < count; i++) {
		auto index = list_data.sel->get_index(i);
		new_child_size += new_size_entries[index];
	}

	ListVector::Reserve(result, new_child_size);
	ListVector::SetListSize(result, new_child_size);

	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_child = ListVector::GetEntry(result);
	auto result_child_data = FlatVector::GetData<T>(result_child);

	auto &result_child_validity = FlatVector::Validity(result_child);

	idx_t result_child_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto l_index = list_data.sel->get_index(i);
		auto new_index = new_size_data.sel->get_index(i);
		if (!list_data.validity.RowIsValid(l_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto values_to_copy = MinValue<idx_t>(list_entries[l_index].length, new_size_entries[new_index]);
		result_entries[i].offset = result_child_offset;
		result_entries[i].length = new_size_entries[new_index];
		for (idx_t j = 0; j < values_to_copy; j++) {
			if (!child_data.validity.RowIsValid(list_entries[l_index].offset + j)) {
				result_child_validity.SetInvalid(result_child_offset);
			} else {
				COPY_FUNCTION::template Operation<T>(child, result_child, list_entries[l_index].offset + j,
				                         result_child_offset);
			}
			result_child_offset++;
		}
		auto new_size_entry = (idx_t)new_size_entries[new_index];
		for (idx_t j = values_to_copy; j < new_size_entry; j++) {
			if (has_default) {
				result_child_data[result_child_offset] = default_value;
			} else {
				result_child_validity.SetInvalid(result_child_offset);
			}
			result_child_offset++;
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void ListResizeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto physical_type = args.data[1].GetType().InternalType();
	switch (physical_type) {
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
//	case PhysicalType::INTERVAL:
//		TemplatedListResizeFunction<interval_t, StringCopyValue>(args, result);
//		break;
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		TemplatedListResizeFunction<int8_t, NestedCopyValue>(args, result);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for physical type %s",
		                              TypeIdToString(physical_type));
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
