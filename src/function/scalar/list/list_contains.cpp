//
// Created by daniel on 24-01-22.
//

#include <iostream>
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression_binder.hpp"


namespace duckdb {

template <class T>
static void TemplatedListContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto count = args.size();

	Vector &list = args.data[0];
	Value value = args.data[1].GetValue(0);

	auto &key_type = ListType::GetChildType(list.GetType());
	if (key_type != LogicalTypeId::SQLNULL) {
		value = value.CastAs(key_type);
	}

	if (list.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(false);
	}


	VectorData list_data;
	list.Orrify(count, list_data);

	auto list_size = ListVector::GetListSize(list);
	auto &child_vector = ListVector::GetEntry(list);
	VectorData child_data;

	child_vector.Orrify(list_size, child_data);
	for (idx_t i = 0; i < count; i++) {
		auto list_index = list_data.sel->get_index(i);
		if (list_data.validity.RowIsValid(list_index)) {
			auto list_entry = ((list_entry_t *)list_data.data)[list_index];
			idx_t child_offset = list_entry.offset;
		   	if (child_data.validity.RowIsValid(child_offset)) {
				auto child_value = ((T *)child_data.data)[child_offset];
				if (child_value == value.GetValue<T>()) {
					printf("GOT HERE");
				} else{
					printf("DID NOT GET HERE");
				}
			}
		}
	}
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
//		case PhysicalType::VARCHAR:
//			TemplatedListContainsFunction(list, StringValue::Get(key), offsets, key.IsNull(), entry.offset, entry.length); 			break;
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
		bound_function.arguments[0] = array;
		bound_function.arguments[1] = value;
		bound_function.return_type = value.id() == LogicalTypeId::SQLNULL ? array : value;
	} else {
		auto child_type =ListType::GetChildType(arguments[0]->return_type);
		D_ASSERT(array.id() == LogicalTypeId::LIST);
		D_ASSERT(value.id() == child_type.id()); // LogicalTypeId::ANY?

		//		LogicalType child_type = LogicalType::SQLNULL;
		//		child_type = LogicalType::MaxLogicalType(child_type, ListType::GetChildType(arguments[0]->return_type));
		//		ExpressionBinder::ResolveParameterType(child_type);

		bound_function.arguments[0] = LogicalType::LIST(move(child_type));
		bound_function.arguments[1] = value; // LogicalType::ANY What should be here. The second argument can be of type any.
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
	set.AddFunction({"list_contains", "array_contains"},GetFunction());
}
}