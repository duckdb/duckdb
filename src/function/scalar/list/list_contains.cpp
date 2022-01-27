//
// Created by daniel on 24-01-22.
//

#include <iostream>
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"


namespace duckdb {

template <class T>
static void TemplatedListContainsStringFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &list = args.data[0];
	Vector &value = args.data[1];

	result.SetVectorType(VectorType::CONSTANT_VECTOR);

//	auto &key_type = ListType::GetChildType(list.GetType());
//	if (key_type != LogicalTypeId::SQLNULL) {
//		value = value.CastAs(key_type);
//	}

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(false);
	}
	VectorData list_data;
	VectorData value_data;

	list.Orrify(count, list_data);
	value.Orrify(count, value_data);

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);

	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (list_data.validity.RowIsValid(list_index)) {
			//			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
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
	auto result_data = ConstantVector::GetData<bool>(result);
	result_data[0] = false;
	return;
}

template <class T>
static void TemplatedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &list = args.data[0];
	Vector &value = args.data[1];

	result.SetVectorType(VectorType::CONSTANT_VECTOR);

//	auto &key_type = ListType::GetChildType(list.GetType());
//	if (key_type != LogicalTypeId::SQLNULL) {
//		value = value.CastAs(key_type);
//	}

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = false;
		return;
	}
	VectorData list_data;
	VectorData value_data;

	list.Orrify(count, list_data);
	value.Orrify(count, value_data);

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);
	if (child_vector.GetType().id() == LogicalTypeId::SQLNULL) {
		auto result_data = ConstantVector::GetData<bool>(result);
		result_data[0] = false;
		return;
	}
	VectorData child_data;
	child_vector.Orrify(list_size, child_data);

	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (list_data.validity.RowIsValid(list_index)) {
//			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
			for (idx_t j = 0; j < list_size; j++) {
				if (child_data.validity.RowIsValid(j)) {
					auto child_value = ((T *)child_data.data)[j];
					if (child_value == ((T *)value_data.data)[0]) {
						auto result_data = ConstantVector::GetData<bool>(result);
						result_data[0] = true;
						return;
					}
				}
			}
		}
	}
	auto result_data = ConstantVector::GetData<bool>(result);
	result_data[0] = false;
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
	default:
		throw InvalidTypeException(args.data[1].GetType().id(), "Invalid type for List Vector Search");
	}
}



static unique_ptr<FunctionData> ListContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	auto &array = arguments[0]->return_type; // change to list
	auto &value = arguments[1]->return_type;
	if (array.id() == LogicalTypeId::SQLNULL && value.id() == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalType::SQLNULL;
	} else if (array.id() == LogicalTypeId::SQLNULL || value.id() == LogicalTypeId::SQLNULL) {
		// In case either the array or the value is NULL, return NULL
		// Similar to behaviour of prestoDB
		bound_function.arguments[0] = array;
		bound_function.arguments[1] = value;
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else {
		auto child_type = ListType::GetChildType(arguments[0]->return_type);
		D_ASSERT(array.id() == LogicalTypeId::LIST);
		if (child_type.id() != LogicalTypeId::SQLNULL) {
			D_ASSERT(value.id() == child_type.id()); // LogicalTypeId::ANY?
		}

		//		LogicalType child_type = LogicalType::SQLNULL;
		//		child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(arguments[0]->return_type));
		//		ExpressionBinder::ResolveParameterType(child_type);

		bound_function.arguments[0] = LogicalType::LIST(move(child_type));
		bound_function.arguments[1] = LogicalType::ANY; // LogicalType::ANY What should be here. The second argument can be of type any.
		bound_function.return_type = LogicalType::BOOLEAN; // What should be here. Boolean
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