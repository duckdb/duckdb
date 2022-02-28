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

template <class T1, class T2, bool IS_NESTED = false>
static void TemplatedContainsOrPosition(DataChunk &args, ExpressionState &state, Vector &result, bool isListContains) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = args.data[0];
	Vector &value_vector = args.data[1];

	// Create a result vector of type T2
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<T2>(result);
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

		if (!list_data.validity.RowIsValid(list_index) || !value_data.validity.RowIsValid(value_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];
		auto source_idx = child_data.sel->get_index(list_entry.offset);

		if (!IS_NESTED) {
			// does not require a comparison of nested types
			auto child_value = FlatVector::GetData<T1>(child_vector);
			auto values = FlatVector::GetData<T1>(value_vector);

			result_entries[list_index] = (isListContains) ? false : 0;
			for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
				auto child_value_idx = source_idx + child_idx;

				if (!child_data.validity.RowIsValid(child_value_idx)) {
					continue;
				}
				if (ValueCompare<T1>(child_value[child_value_idx], values[value_index])) {
					result_entries[list_index] = (isListContains) ? true : child_idx + 1;
					break; // Found value in list, no need to look further
				}
			}
		} else {
			result_entries[list_index] = (isListContains) ? false : 0;
			for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {
				auto child_value_idx = source_idx + child_idx;
				if (!child_data.validity.RowIsValid(child_value_idx)) {
					continue;
				}
				if (ValueCompare<Value>(child_vector.GetValue(child_value_idx), value_vector.GetValue(value_index))) {
					result_entries[list_index] = (isListContains) ? true : child_idx + 1;
					break; // Found value in list, no need to look further
				}
			}
		}
	}
}

template <class T>
static void ListContainsOrPosition(DataChunk &args, ExpressionState &state, Vector &result, bool isListContains) {
	switch (args.data[1].GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedContainsOrPosition<int8_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::INT16:
		TemplatedContainsOrPosition<int16_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::INT32:
		TemplatedContainsOrPosition<int32_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::INT64:
		TemplatedContainsOrPosition<int64_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::INT128:
		TemplatedContainsOrPosition<hugeint_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::UINT8:
		TemplatedContainsOrPosition<uint8_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::UINT16:
		TemplatedContainsOrPosition<uint16_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::UINT32:
		TemplatedContainsOrPosition<uint32_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::UINT64:
		TemplatedContainsOrPosition<uint64_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::FLOAT:
		TemplatedContainsOrPosition<float, T>(args, state, result, isListContains);
		break;
	case PhysicalType::DOUBLE:
		TemplatedContainsOrPosition<double, T>(args, state, result, isListContains);
		break;
	case PhysicalType::VARCHAR:
		TemplatedContainsOrPosition<string_t, T>(args, state, result, isListContains);
		break;
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		TemplatedContainsOrPosition<int8_t, T, true>(args, state, result, isListContains);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for this type");
	}
}

static void ListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListContainsOrPosition<bool>(args, state, result, true);
}

static void ListPositionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListContainsOrPosition<int32_t>(args, state, result, false);
}

static unique_ptr<FunctionData> ListContainsOrPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments,
                                                           bool isListContains) {
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

		// list_contains and list_position only differ in their return type
		bound_function.return_type = (isListContains) ? LogicalType::BOOLEAN : LogicalType::INTEGER;
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind(context, bound_function, arguments, true);
}

static unique_ptr<FunctionData> ListPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind(context, bound_function, arguments, false);
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::BOOLEAN,                                    // return type
	                      ListContainsFunction, false, ListContainsBind, nullptr);
}

ScalarFunction ListPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::INTEGER,                                    // return type
	                      ListPositionFunction, false, ListPositionBind, nullptr);
}

void ListContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_contains", "array_contains", "list_has", "array_has"}, GetFunction());
}

void ListPositionFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_position", "list_indexof", "array_position", "array_indexof"}, GetFunction());
}
} // namespace duckdb