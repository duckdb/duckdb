#include "duckdb/function/scalar/enum_functions.hpp"

namespace duckdb {

static void EnumFirstFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	auto &enum_vector = EnumType::GetValuesInsertOrder(input.GetTypes()[0]);
	auto val = Value(enum_vector.GetValue(0));
	result.Reference(val);
}

static void EnumLastFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	auto enum_size = EnumType::GetSize(input.GetTypes()[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(input.GetTypes()[0]);
	auto val = Value(enum_vector.GetValue(enum_size - 1));
	result.Reference(val);
}

static void EnumRangeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 1);
	auto enum_size = EnumType::GetSize(input.GetTypes()[0]);
	auto &enum_vector = EnumType::GetValuesInsertOrder(input.GetTypes()[0]);
	vector<Value> enum_values;
	for (idx_t i = 0; i < enum_size; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	auto val = Value::LIST(enum_values);
	result.Reference(val);
}

static void EnumRangeBoundaryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.GetTypes().size() == 2);
	idx_t start, end;
	auto first_param = input.GetValue(0, 0);
	auto second_param = input.GetValue(1, 0);

	auto &enum_vector = first_param.IsNull() ? EnumType::GetValuesInsertOrder(input.GetTypes()[1])
	                                         : EnumType::GetValuesInsertOrder(input.GetTypes()[0]);

	if (first_param.IsNull()) {
		start = 0;
	} else {
		start = first_param.GetValue<uint32_t>();
	}
	if (second_param.IsNull()) {
		end = EnumType::GetSize(input.GetTypes()[0]);
	} else {
		end = second_param.GetValue<uint32_t>() + 1;
	}
	vector<Value> enum_values;
	for (idx_t i = start; i < end; i++) {
		enum_values.emplace_back(enum_vector.GetValue(i));
	}
	Value val;
	if (enum_values.empty()) {
		val = Value::EMPTYLIST(LogicalType::VARCHAR);
	} else {
		val = Value::LIST(enum_values);
	}
	result.Reference(val);
}

unique_ptr<FunctionData> BindEnumFunction(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::ENUM) {
		throw BinderException("This function needs an ENUM as an argument");
	}
	return nullptr;
}

unique_ptr<FunctionData> BindEnumRangeBoundaryFunction(ClientContext &context, ScalarFunction &bound_function,
                                                       vector<unique_ptr<Expression>> &arguments) {
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

void EnumFirst::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("enum_first", {LogicalType::ANY}, LogicalType::VARCHAR, EnumFirstFunction, false,
	                               BindEnumFunction));
}

void EnumLast::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("enum_last", {LogicalType::ANY}, LogicalType::VARCHAR, EnumLastFunction, false,
	                               BindEnumFunction));
}

void EnumRange::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("enum_range", {LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR),
	                               EnumRangeFunction, false, BindEnumFunction));
}

void EnumRangeBoundary::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("enum_range_boundary", {LogicalType::ANY, LogicalType::ANY},
	                               LogicalType::LIST(LogicalType::VARCHAR), EnumRangeBoundaryFunction, false,
	                               BindEnumRangeBoundaryFunction));
}

} // namespace duckdb
