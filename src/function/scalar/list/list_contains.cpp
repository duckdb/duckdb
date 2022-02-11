#include <iostream>
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

static void SetResultFalse(Vector &result) {
	auto result_data = FlatVector::GetData<bool>(result);
	result_data[0] = false;
	return;
}

template <class T>
static inline bool ValueCompare(const T &left, const T &right) {
	return left == right;
}

template <>
inline bool ValueCompare(const string_t &left, const string_t &right) {
	return StringComparisonOperators::EqualsOrNot<false>(left, right);
}

template <class T>
static void TemplatedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = args.data[0];
	Vector &value = args.data[1];
	VectorData value_data;
	value.Orrify(count, value_data);

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		SetResultFalse(result);
		return;
	}

	auto list_size = ListVector::GetListSize(list);
	if (list_size == 0) { // empty list will never contain a value
		SetResultFalse(result);
		return;
	}
	auto &child_vector = ListVector::GetEntry(list);
	if (child_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		SetResultFalse(result);
		return;
	}
	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	VectorData list_data;
	list.Orrify(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<bool>(result); // Create a vector of bool
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index)) {
			result_validity.SetInvalid(i);
			continue;
		}
		result_entries[list_index] = false;

		const auto &entry = list_entries[list_index];
		auto source_idx = child_data.sel->get_index(entry.offset);
		auto child_value = FlatVector::GetData<T>(child_vector);

		for (idx_t child_idx = 0; child_idx < entry.length; child_idx++) {
			auto value_idx = source_idx + child_idx;
			if (!child_data.validity.RowIsValid(value_idx)) {
				continue;
			}
			auto actual_value = child_value[value_idx];
			if (ValueCompare(actual_value, ((T *)value_data.data)[0])) {
				result_entries[list_index] = true;
				break; // Found value in list, no need to look further
			}
		}
	}
	return;
}

static void ListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	switch (args.data[1].GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedListContainsFunction<int8_t>(args, state, result);
		break;
	case PhysicalType::INT16:
		TemplatedListContainsFunction<int16_t>(args, state, result);
		break;
	case PhysicalType::INT32:
		TemplatedListContainsFunction<int32_t>(args, state, result);
		break;
	case PhysicalType::INT64:
		TemplatedListContainsFunction<int64_t>(args, state, result);
		break;
	case PhysicalType::INT128:
		TemplatedListContainsFunction<hugeint_t>(args, state, result);
		break;
	case PhysicalType::UINT8:
		TemplatedListContainsFunction<uint8_t>(args, state, result);
		break;
	case PhysicalType::UINT16:
		TemplatedListContainsFunction<uint16_t>(args, state, result);
		break;
	case PhysicalType::UINT32:
		TemplatedListContainsFunction<uint32_t>(args, state, result);
		break;
	case PhysicalType::UINT64:
		TemplatedListContainsFunction<uint64_t>(args, state, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedListContainsFunction<float>(args, state, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedListContainsFunction<double>(args, state, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedListContainsFunction<string_t>(args, state, result);
		break;
	case PhysicalType::LIST:
		throw NotImplementedException("This function has not yet been implemented for nested types");
	default:
		throw InvalidTypeException(args.data[1].GetType().id(), "Invalid type for List Contains");
	}
}

static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	const auto &list = arguments[0]->return_type; // change to list
	const auto &value = arguments[1]->return_type;
	if (list.id() == LogicalTypeId::SQLNULL && value.id() == LogicalTypeId::SQLNULL) {
		bound_function.arguments[0] = LogicalType::SQLNULL;
		bound_function.arguments[1] = LogicalType::SQLNULL;
		bound_function.return_type = LogicalType::SQLNULL;
	} else if (list.id() == LogicalTypeId::SQLNULL || value.id() == LogicalTypeId::SQLNULL) {
		// In case either the list or the value is NULL, return NULL
		// Similar to behaviour of prestoDB
		bound_function.arguments[0] = list;
		bound_function.arguments[1] = value;
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else {
		D_ASSERT(list.id() == LogicalTypeId::LIST);

		auto const &child_type = ListType::GetChildType(arguments[0]->return_type);
		auto max_child_type = LogicalType::MaxLogicalType(child_type, value);
		ExpressionBinder::ResolveParameterType(max_child_type);
		auto list_type = LogicalType::LIST(max_child_type);

		bound_function.arguments[0] = list_type;
		bound_function.arguments[1] = value == max_child_type ? value : max_child_type;
		bound_function.return_type = LogicalType::BOOLEAN;
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::BOOLEAN,                                    // return type
	                      ListContainsFunction, false, ListContainsBind, nullptr);
}

void ListContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_contains", "array_contains", "list_has", "array_has"}, GetFunction());
}
} // namespace duckdb