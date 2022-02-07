//
// Created by daniel on 24-01-22.
//

#include <iostream>
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"


namespace duckdb {

static void SetResultFalse(Vector &result) {
	auto result_data = ConstantVector::GetData<bool>(result);
	result_data[0] = false;
	return;
}

template <class T>
static void TemplatedListContainsStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &list = args.data[0];
	Vector &value = args.data[1];

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

	result.SetVectorType(VectorType::CONSTANT_VECTOR);

	VectorData list_data;
	VectorData value_data;

	list.Orrify(count, list_data);
	value.Orrify(count, value_data);

	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (list_data.validity.RowIsValid(list_index)) {
			for (idx_t j = 0; j < list_size; j++) {
				if (child_data.validity.RowIsValid(j)) {
					auto child_value = ((T *)child_data.data)[j];
					if (StringComparisonOperators::EqualsOrNot<false>(child_value, ((T *)value_data.data)[0])) {
						auto result_data = ConstantVector::GetData<bool>(result);
						result_data[0] = true;
						return;
					}
				}
			}
		}
	}
	SetResultFalse(result);
	return;
}


template <class T>
static void TemplatedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &list = args.data[0];
	Vector &value = args.data[1];

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

	result.SetVectorType(VectorType::FLAT_VECTOR);

	VectorData list_data;
	VectorData value_data;

	list.Orrify(count, list_data);
	value.Orrify(count, value_data);

	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		auto result_data = FlatVector::GetData<bool>(result);
		result_data[list_index] = false;
		if (!list_data.validity.RowIsValid(list_index)) {
			continue;
		}
		for (idx_t child_idx = 0; child_idx < list_size; child_idx++) {
			if (!child_data.validity.RowIsValid(child_idx)) {
				continue;
			}
			auto child_value = ((T *)child_data.data)[child_idx];
			if (child_value == ((T *)value_data.data)[0]) {
				result_data[list_index] = true;
				continue;
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
		TemplatedListContainsStringFunction<string_t>(args, state, result);
		break;
	case PhysicalType::LIST:
		throw NotImplementedException("This function has not yet been implemented for nested types" );
	default:
		throw InvalidTypeException(args.data[1].GetType().id(), "Invalid type for List Vector Search");
	}
}


static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	auto &list = arguments[0]->return_type; // change to list
	auto &value = arguments[1]->return_type;
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

		auto child_type = ListType::GetChildType(arguments[0]->return_type);
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
	                      LogicalType::BOOLEAN,                         // return type
	                      ListContainsFunction, false, ListContainsBind, nullptr);
}

void ListContainsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"list_contains", "array_contains", "list_has", "array_has"},GetFunction());
}
}