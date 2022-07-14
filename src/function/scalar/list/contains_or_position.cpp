#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

template <class T>
static inline bool ValueEqualsOrNot(const T &left, const T &right) {
	return left == right;
}

template <>
inline bool ValueEqualsOrNot(const string_t &left, const string_t &right) {
	return StringComparisonOperators::EqualsOrNot<false>(left, right);
}

struct ContainsFunctor {
	static inline bool Initialize() {
		return false;
	}
	static inline bool UpdateResultEntries(idx_t child_idx) {
		return true;
	}
};

struct PositionFunctor {
	static inline int32_t Initialize() {
		return 0;
	}
	static inline int32_t UpdateResultEntries(idx_t child_idx) {
		return child_idx + 1;
	}
};

template <class CHILD_TYPE, class RETURN_TYPE, class OP>
static void TemplatedContainsOrPosition(DataChunk &args, ExpressionState &state, Vector &result,
                                        bool is_nested = false) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = args.data[0];
	Vector &value_vector = args.data[1];

	// Create a result vector of type RETURN_TYPE
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<RETURN_TYPE>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(list_size, child_data);

	UnifiedVectorFormat list_data;
	list.ToUnifiedFormat(count, list_data);
	auto list_entries = (list_entry_t *)list_data.data;

	UnifiedVectorFormat value_data;
	value_vector.ToUnifiedFormat(count, value_data);

	// not required for a comparison of nested types
	auto child_value = FlatVector::GetData<CHILD_TYPE>(child_vector);
	auto values = FlatVector::GetData<CHILD_TYPE>(value_vector);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		auto value_index = value_data.sel->get_index(i);

		if (!list_data.validity.RowIsValid(list_index) || !value_data.validity.RowIsValid(value_index)) {
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];

		result_entries[i] = OP::Initialize();
		for (idx_t child_idx = 0; child_idx < list_entry.length; child_idx++) {

			auto child_value_idx = child_data.sel->get_index(list_entry.offset + child_idx);
			if (!child_data.validity.RowIsValid(child_value_idx)) {
				continue;
			}

			if (!is_nested) {
				if (ValueEqualsOrNot<CHILD_TYPE>(child_value[child_value_idx], values[value_index])) {
					result_entries[i] = OP::UpdateResultEntries(child_idx);
					break; // Found value in list, no need to look further
				}
			} else {
				// FIXME: using Value is less efficient than modifying the vector comparison code
				// to more efficiently compare nested types
				if (ValueEqualsOrNot<Value>(child_vector.GetValue(child_value_idx),
				                            value_vector.GetValue(value_index))) {
					result_entries[i] = OP::UpdateResultEntries(child_idx);
					break; // Found value in list, no need to look further
				}
			}
		}
	}
}

template <class T, class OP>
static void ListContainsOrPosition(DataChunk &args, ExpressionState &state, Vector &result) {
	switch (args.data[1].GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedContainsOrPosition<int8_t, T, OP>(args, state, result);
		break;
	case PhysicalType::INT16:
		TemplatedContainsOrPosition<int16_t, T, OP>(args, state, result);
		break;
	case PhysicalType::INT32:
		TemplatedContainsOrPosition<int32_t, T, OP>(args, state, result);
		break;
	case PhysicalType::INT64:
		TemplatedContainsOrPosition<int64_t, T, OP>(args, state, result);
		break;
	case PhysicalType::INT128:
		TemplatedContainsOrPosition<hugeint_t, T, OP>(args, state, result);
		break;
	case PhysicalType::UINT8:
		TemplatedContainsOrPosition<uint8_t, T, OP>(args, state, result);
		break;
	case PhysicalType::UINT16:
		TemplatedContainsOrPosition<uint16_t, T, OP>(args, state, result);
		break;
	case PhysicalType::UINT32:
		TemplatedContainsOrPosition<uint32_t, T, OP>(args, state, result);
		break;
	case PhysicalType::UINT64:
		TemplatedContainsOrPosition<uint64_t, T, OP>(args, state, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedContainsOrPosition<float, T, OP>(args, state, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedContainsOrPosition<double, T, OP>(args, state, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedContainsOrPosition<string_t, T, OP>(args, state, result);
		break;
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
		TemplatedContainsOrPosition<int8_t, T, OP>(args, state, result, true);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for this type");
	}
}

static void ListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListContainsOrPosition<bool, ContainsFunctor>(args, state, result);
}

static void ListPositionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	return ListContainsOrPosition<int32_t, PositionFunctor>(args, state, result);
}

template <LogicalTypeId RETURN_TYPE>
static unique_ptr<FunctionData> ListContainsOrPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	const auto &list = arguments[0]->return_type; // change to list
	const auto &value = arguments[1]->return_type;
	if (list.id() == LogicalTypeId::UNKNOWN) {
		bound_function.return_type = RETURN_TYPE;
		if (value.id() != LogicalTypeId::UNKNOWN) {
			// only list is a parameter, cast it to a list of value type
			bound_function.arguments[0] = LogicalType::LIST(value);
			bound_function.arguments[1] = value;
		}
	} else if (value.id() == LogicalTypeId::UNKNOWN) {
		// only value is a parameter: we expect the child type of list
		auto const &child_type = ListType::GetChildType(list);
		bound_function.arguments[0] = list;
		bound_function.arguments[1] = child_type;
		bound_function.return_type = RETURN_TYPE;
	} else {
		auto const &child_type = ListType::GetChildType(list);
		auto max_child_type = LogicalType::MaxLogicalType(child_type, value);
		auto list_type = LogicalType::LIST(max_child_type);

		bound_function.arguments[0] = list_type;
		bound_function.arguments[1] = value == max_child_type ? value : max_child_type;

		// list_contains and list_position only differ in their return type
		bound_function.return_type = RETURN_TYPE;
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind<LogicalType::BOOLEAN>(context, bound_function, arguments);
}

static unique_ptr<FunctionData> ListPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	return ListContainsOrPositionBind<LogicalType::INTEGER>(context, bound_function, arguments);
}

ScalarFunction ListContainsFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::BOOLEAN,                                    // return type
	                      ListContainsFunction, ListContainsBind, nullptr);
}

ScalarFunction ListPositionFun::GetFunction() {
	return ScalarFunction({LogicalType::LIST(LogicalType::ANY), LogicalType::ANY}, // argument list
	                      LogicalType::INTEGER,                                    // return type
	                      ListPositionFunction, ListPositionBind, nullptr);
}

void ListContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_contains", "array_contains", "list_has", "array_has"}, GetFunction());
}

void ListPositionFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_position", "list_indexof", "array_position", "array_indexof"}, GetFunction());
}
} // namespace duckdb
