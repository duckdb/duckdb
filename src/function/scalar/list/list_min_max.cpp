#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

template <class T>
static inline bool ValueCompareMin(const T &left, const T &right) {
	return left < right;
}

template <>
inline bool ValueCompareMin(const Value &left, const Value &right) {
	return left < right;
}

template <class T, bool HEAP_REF = false, bool IS_NESTED = false>
void ListMinMaxTemplate(idx_t count, VectorData &list_data, Vector &child_vector,
                         idx_t list_size, Vector &result, bool isMin) {
							 
	VectorData child_data;
	child_vector.Orrify(list_size, child_data);
	auto list_entries = (list_entry_t *)list_data.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);

	// heap-ref once
	if (HEAP_REF) {
		StringVector::AddHeapReference(result, child_vector);
	}

    for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index)) {
			result_mask.SetInvalid(i);
			continue;
		}

		auto &list_entry = list_entries[list_index];
		auto source_idx = child_data.sel->get_index(list_entry.offset);

        // find first valid list element
        int64_t first_valid_elem = -1;
        for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
            auto child_value_idx = source_idx + child_idx;
            if (child_data.validity.RowIsValid(child_value_idx)) {
                first_valid_elem = child_idx;
				break;
            }
        }

        if (first_valid_elem == -1) {
            // TODO: what is the minimum/maximum of an empty list?
            result_mask.SetInvalid(i);
        } else {
			// iterate the rest of the list
			if (!IS_NESTED) {
				T *result_data;
				result_data = FlatVector::GetData<T>(result);

				auto child_value = FlatVector::GetData<T>(child_vector);
				result_data[i] = ((T *)child_data.data)[source_idx + first_valid_elem];
				for (idx_t child_idx = first_valid_elem + 1; child_idx < list_entry.length; child_idx++) {
					auto child_value_idx = source_idx + child_idx;
					if (!child_data.validity.RowIsValid(child_value_idx)) {
						continue;
					}
					auto isLess = ValueCompareMin<T>(child_value[child_value_idx], result_data[list_index]);
					if ((isMin && isLess) || (!isMin && !isLess)) {
						result_data[i] = ((T *)child_data.data)[child_value_idx];
					}
				}
			} else {
				list_entry_t *result_data;
				result_data = FlatVector::GetData<list_entry_t>(result);
		
				result_data[i] = ((list_entry_t *)child_data.data)[source_idx + first_valid_elem];
				idx_t last_child_value_idx = source_idx + first_valid_elem;
				for (idx_t child_idx = first_valid_elem + 1; child_idx < list_entry.length; child_idx++) {
					auto child_value_idx = source_idx + child_idx;
					if (!child_data.validity.RowIsValid(child_value_idx)) {
						continue;
					}
					auto isLess = ValueCompareMin<Value>(child_vector.GetValue(child_value_idx), child_vector.GetValue(last_child_value_idx));
					if ((isMin && isLess) || (!isMin && !isLess)) {
						result_data[i] = ((list_entry_t *)child_data.data)[child_value_idx];
						last_child_value_idx = child_value_idx;
					}
				}
			}            
        }
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}
}

static void ExecuteListMinMaxInternal(const idx_t count, VectorData &list, Vector &child_vector,
                                       idx_t list_size, Vector &result, bool isMin) {
	D_ASSERT(child_vector.GetType() == result.GetType());
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ListMinMaxTemplate<int8_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::INT16:
		ListMinMaxTemplate<int16_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::INT32:
		ListMinMaxTemplate<int32_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::INT64:
		ListMinMaxTemplate<int64_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::INT128:
		ListMinMaxTemplate<hugeint_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::UINT8:
		ListMinMaxTemplate<uint8_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::UINT16:
		ListMinMaxTemplate<uint16_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::UINT32:
		ListMinMaxTemplate<uint32_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::UINT64:
		ListMinMaxTemplate<uint64_t>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::FLOAT:
		ListMinMaxTemplate<float>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::DOUBLE:
		ListMinMaxTemplate<double>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::VARCHAR:
		ListMinMaxTemplate<string_t, true>(count, list, child_vector, list_size, result, isMin);
		break;
	case PhysicalType::INTERVAL:
		ListMinMaxTemplate<interval_t>(count, list, child_vector, list_size, result, isMin);
		break;
	// TODO: what about MAP types? A map is a struct of two lists (key, value)...
	//case PhysicalType::STRUCT: { 
		// TODO... this is more complex because the corresponding elements 
		// in the child lists have to be compared
		//break;
	//}
	case PhysicalType::LIST: {
		// nested list: we have to reference the child
		auto &child_child_list = ListVector::GetEntry(child_vector);
		ListVector::GetEntry(result).Reference(child_child_list);
		ListVector::SetListSize(result, ListVector::GetListSize(child_vector));
		ListMinMaxTemplate<int8_t, false, true>(count, list, child_vector, list_size, result, isMin);
		break;
	}
	default:
        if (isMin) {
			throw NotImplementedException("Unimplemented type for LIST_MIN");
		} else {
			throw NotImplementedException("Unimplemented type for LIST_MAX");
		}
	}
}

static void ExecuteListMinMax(Vector &result, Vector &list, const idx_t count, bool isMin) {
	D_ASSERT(list.GetType().id() == LogicalTypeId::LIST);
	VectorData list_data;

	list.Orrify(count, list_data);
	ExecuteListMinMaxInternal(count, list_data, ListVector::GetEntry(list),
	                           ListVector::GetListSize(list), result, isMin);
	result.Verify(count);
}

static void ListMinMaxFunction(DataChunk &args, ExpressionState &state, Vector &result, bool isMin) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		if (args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::FLAT_VECTOR);
		}
	}

	Vector &base = args.data[0];
	switch (base.GetType().id()) {
	case LogicalTypeId::LIST:
		ExecuteListMinMax(result, base, count, isMin);
		break;
	case LogicalTypeId::SQLNULL: // TODO: we return null if the column entry is null?
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static void ListMinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListMinMaxFunction(args, state, result, true);
}

static void ListMaxFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListMinMaxFunction(args, state, result, false);
}

static unique_ptr<FunctionData> ListMinMaxBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 1);
	if (arguments[0]->return_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
	} else {
		D_ASSERT(LogicalTypeId::LIST == arguments[0]->return_type.id());
		// list min/max returns the child type of the list as return type
		bound_function.return_type = ListType::GetChildType(arguments[0]->return_type);
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListMaxFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::ANY,
	                      ListMaxFunction, false, ListMinMaxBind, nullptr);
}

ScalarFunction ListMinFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY)}, LogicalType::ANY,
	                      ListMinFunction, false, ListMinMaxBind, nullptr);
}

void ListMaxFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_max", "array_max"}, GetFunction());
}

void ListMinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_min", "array_min"}, GetFunction());
}

} // namespace duckdb
