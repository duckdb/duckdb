#pragma once

#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

template <class CHILD_TYPE, class RETURN_TYPE, class OP, class LIST_ACCESSOR>
static void TemplatedContainsOrPosition(DataChunk &args, Vector &result, bool is_nested = false) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();
	Vector &list = LIST_ACCESSOR::GetList(args.data[0]);
	Vector &value_vector = args.data[1];

	// Create a result vector of type RETURN_TYPE
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<RETURN_TYPE>(result);
	auto &result_validity = FlatVector::Validity(result);

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result_validity.SetInvalid(0);
		return;
	}

	auto list_size = LIST_ACCESSOR::GetListSize(list);
	auto &child_vector = LIST_ACCESSOR::GetEntry(list);

	UnifiedVectorFormat child_data;
	child_vector.ToUnifiedFormat(list_size, child_data);

	UnifiedVectorFormat list_data;
	list.ToUnifiedFormat(count, list_data);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_data);

	UnifiedVectorFormat value_data;
	value_vector.ToUnifiedFormat(count, value_data);

	// not required for a comparison of nested types
	auto child_value = UnifiedVectorFormat::GetData<CHILD_TYPE>(child_data);
	auto values = UnifiedVectorFormat::GetData<CHILD_TYPE>(value_data);

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
				if (Equals::Operation(child_value[child_value_idx], values[value_index])) {
					result_entries[i] = OP::UpdateResultEntries(child_idx);
					break; // Found value in list, no need to look further
				}
			} else {
				// FIXME: using Value is less efficient than modifying the vector comparison code
				// to more efficiently compare nested types

				// Note: When using GetValue we don't first apply the selection vector
				// because it is already done inside GetValue
				auto lvalue = child_vector.GetValue(list_entry.offset + child_idx);
				auto rvalue = value_vector.GetValue(i);
				if (Value::NotDistinctFrom(lvalue, rvalue)) {
					result_entries[i] = OP::UpdateResultEntries(child_idx);
					break; // Found value in list, no need to look further
				}
			}
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

template <LogicalTypeId RETURN_TYPE>
unique_ptr<FunctionData> ListContainsOrPositionBind(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	// If the first argument is an array, cast it to a list
	arguments[0] = BoundCastExpression::AddArrayCastToList(context, std::move(arguments[0]));

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
		LogicalType max_child_type;
		if (!LogicalType::TryGetMaxLogicalType(context, child_type, value, max_child_type)) {
			throw BinderException(
			    "Cannot get list_position of element of type %s in a list of type %s[] - an explicit cast is required",
			    value.ToString(), child_type.ToString());
		}
		auto list_type = LogicalType::LIST(max_child_type);

		bound_function.arguments[0] = list_type;
		bound_function.arguments[1] = value == max_child_type ? value : max_child_type;

		// list_contains and list_position only differ in their return type
		bound_function.return_type = RETURN_TYPE;
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

template <class T, class OP, class LIST_ACCESSOR>
void ListContainsOrPosition(DataChunk &args, Vector &result) {
	const auto physical_type = args.data[1].GetType().InternalType();
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedContainsOrPosition<int8_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::INT16:
		TemplatedContainsOrPosition<int16_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::INT32:
		TemplatedContainsOrPosition<int32_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::INT64:
		TemplatedContainsOrPosition<int64_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::INT128:
		TemplatedContainsOrPosition<hugeint_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::UINT8:
		TemplatedContainsOrPosition<uint8_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::UINT16:
		TemplatedContainsOrPosition<uint16_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::UINT32:
		TemplatedContainsOrPosition<uint32_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::UINT64:
		TemplatedContainsOrPosition<uint64_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::UINT128:
		TemplatedContainsOrPosition<uhugeint_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedContainsOrPosition<float, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedContainsOrPosition<double, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::VARCHAR:
		TemplatedContainsOrPosition<string_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedContainsOrPosition<interval_t, T, OP, LIST_ACCESSOR>(args, result);
		break;
	case PhysicalType::STRUCT:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		TemplatedContainsOrPosition<int8_t, T, OP, LIST_ACCESSOR>(args, result, true);
		break;
	default:
		throw NotImplementedException("This function has not been implemented for logical type %s",
		                              TypeIdToString(physical_type));
	}
}

} // namespace duckdb
