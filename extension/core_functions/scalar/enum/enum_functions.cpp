#include "core_functions/scalar/enum_functions.hpp"

namespace duckdb {

static void EnumFirstFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	auto val = Value(enum_vector.GetValue(0));
	result.Reference(val);
}

static void EnumLastFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto enum_size = EnumType::GetSize(types[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	auto val = Value(enum_vector.GetValue(enum_size - 1));
	result.Reference(val);
}

static void EnumRangeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 1);
	auto enum_size = EnumType::GetSize(types[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(types[0]);
	vector<Value> enum_values;
	for (idx_t i = 0; i < enum_size; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	auto val = Value::LIST(LogicalType::VARCHAR, enum_values);
	result.Reference(val);
}

static void EnumRangeBoundaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto types = input.GetTypes();
	D_ASSERT(types.size() == 2);
	idx_t start, end;
	auto first_param = input.GetValue(0, 0);
	auto second_param = input.GetValue(1, 0);

	auto &enum_vector =
	    first_param.IsNull() ? EnumType::GetValuesInsertOrder(types[1]) : EnumType::GetValuesInsertOrder(types[0]);

	if (first_param.IsNull()) {
		start = 0;
	} else {
		start = first_param.GetValue<uint32_t>();
	}
	if (second_param.IsNull()) {
		end = EnumType::GetSize(types[0]);
	} else {
		end = second_param.GetValue<uint32_t>() + 1;
	}
	vector<Value> enum_values;
	for (idx_t i = start; i < end; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	auto val = Value::LIST(LogicalType::VARCHAR, enum_values);
	result.Reference(val);
}

static void EnumCodeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	result.Reinterpret(input.data[0]);
}

static void CheckEnumParameter(const Expression &expr) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
}

static unique_ptr<FunctionData> BindEnumFunction(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	return nullptr;
}

static unique_ptr<FunctionData> BindEnumCodeFunction(ClientContext &context, ScalarFunction &bound_function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw BinderException("This function needs an ENUM as an argument");
	}

	auto phy_type = EnumType::GetPhysicalType(arguments[0]->return_type);
	switch (phy_type) {
	case PhysicalType::UINT8:
		bound_function.SetReturnType(LogicalType(LogicalTypeId::UTINYINT));
		break;
	case PhysicalType::UINT16:
		bound_function.SetReturnType(LogicalType(LogicalTypeId::USMALLINT));
		break;
	case PhysicalType::UINT32:
		bound_function.SetReturnType(LogicalType(LogicalTypeId::UINTEGER));
		break;
	case PhysicalType::UINT64:
		bound_function.SetReturnType(LogicalType(LogicalTypeId::UBIGINT));
		break;
	default:
		throw InternalException("Unsupported Enum Internal Type");
	}

	return nullptr;
}

static unique_ptr<FunctionData> BindEnumRangeBoundaryFunction(ClientContext &context, ScalarFunction &bound_function,
                                                              vector<unique_ptr<Expression>> &arguments) {
	CheckEnumParameter(*arguments[0]);
	CheckEnumParameter(*arguments[1]);
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM && arguments[0]->return_type != LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[1]->return_type.id() != LogicalTypeId::ENUM && arguments[1]->return_type != LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[0]->return_type == LogicalType::SQLNULL && arguments[1]->return_type == LogicalType::SQLNULL) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	if (arguments[0]->return_type.id() == LogicalTypeId::ENUM &&
	    arguments[1]->return_type.id() == LogicalTypeId::ENUM &&
	    arguments[0]->return_type != arguments[1]->return_type) {
		throw BinderException("The parameters need to link to ONLY one enum OR be NULL ");
	}
	return nullptr;
}

ScalarFunction EnumFirstFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, EnumFirstFunction, BindEnumFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

ScalarFunction EnumLastFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::VARCHAR, EnumLastFunction, BindEnumFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

ScalarFunction EnumCodeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::ANY, EnumCodeFunction, BindEnumCodeFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

ScalarFunction EnumRangeFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), EnumRangeFunction,
	                          BindEnumFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

ScalarFunction EnumRangeBoundaryFun::GetFunction() {
	auto fun = ScalarFunction({LogicalType::ANY, LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR),
	                          EnumRangeBoundaryFunction, BindEnumRangeBoundaryFunction);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
