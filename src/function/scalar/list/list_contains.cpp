#include <iostream>
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

template <class T>
static inline bool ValueCompare(const T &left, const T &right) {
	return left == right;
}

template <>
inline bool ValueCompare(const string_t &left, const string_t &right) {
	return StringComparisonOperators::EqualsOrNot<false>(left, right);
}

template <>
inline bool ValueCompare(const Value &left, const Value &right) {
	return left == right;
}

template <class T>
static void TemplatedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = args.data[0];
	Vector &value_vector = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<bool>(result); // Create a vector of bool
	auto &result_validity = FlatVector::Validity(result);

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	VectorData list_data;
	list.Orrify(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	VectorData value_data;
	value_vector.Orrify(count, value_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		auto value_index = value_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index) or !value_data.validity.RowIsValid(value_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];
		auto source_idx = child_data.sel->get_index(list_entry.offset);
		auto child_value = FlatVector::GetData<T>(child_vector);

		auto values = FlatVector::GetData<T>(value_vector);
		result_entries[list_index] = false;
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto child_value_idx = source_idx + child_idx;

			if (!child_data.validity.RowIsValid(child_value_idx)) {
				continue;
			}
			if (ValueCompare<T>(child_value[child_value_idx], values[value_index])) {
				result_entries[list_index] = true;
				break; // Found value in list, no need to look further
			}
		}
	}
	return;
}

static void NestedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = args.data[0];
	Vector &value_vector = args.data[1];

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<bool>(result); // Create a vector of bool
	auto &result_validity = FlatVector::Validity(result);

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	VectorData list_data;
	list.Orrify(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	VectorData value_data;
	value_vector.Orrify(count, value_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		auto value_index = value_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index) or !value_data.validity.RowIsValid(value_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];
		auto source_idx = child_data.sel->get_index(list_entry.offset);

		result_entries[list_index] = false;
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
			auto child_value_idx = source_idx + child_idx;
			if (!child_data.validity.RowIsValid(child_value_idx)) {
				continue;
			}
			if (ValueCompare<Value>(child_vector.GetValue(child_value_idx), value_vector.GetValue(value_index))) {
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
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		NestedListContainsFunction(args, state, result);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for this type");
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