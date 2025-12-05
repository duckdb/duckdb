#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

namespace duckdb {

static void SwitchFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("Switch function should have been rewritten to a case expression");
}

unique_ptr<Expression> SwitchBindDefaultExpression(FunctionBindExpressionInput &input) {
	Printer::Print("Got here and then probably crash");

	throw InternalException("Switch function got to the bind expression");
	return make_uniq<BoundCaseExpression>(LogicalType::SQLNULL);
}

unique_ptr<Expression> SwitchBindMissingDefaultExpression(FunctionBindExpressionInput &input) {
	Printer::Print("Got here and then probably crash");

	throw InternalException("Switch function got to the bind expression");
	return make_uniq<BoundCaseExpression>(LogicalType::SQLNULL);
}

ScalarFunctionSet SwitchFun::GetFunctions() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");
	ScalarFunctionSet func_set;
	auto switch_missing_default = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type)},
		val_type, SwitchFunc);
	switch_missing_default.SetBindExpressionCallback(SwitchBindMissingDefaultExpression);
	func_set.AddFunction(std::move(switch_missing_default));
	auto switch_missing = ScalarFunction({key_type, LogicalType::MAP(key_type, val_type), val_type}, val_type,
	                   SwitchFunc);
	switch_missing.SetBindExpressionCallback(SwitchBindDefaultExpression);
	func_set.AddFunction(std::move(switch_missing));
	return func_set;
}
}
