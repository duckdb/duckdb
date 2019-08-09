#include "function/scalar_function/math_functions.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {

bool single_numeric_argument(vector<SQLType> &arguments) {
	if (arguments.size() != 1) {
		return false;
	}
	switch (arguments[0].id) {
	case SQLTypeId::TINYINT:
	case SQLTypeId::SMALLINT:
	case SQLTypeId::INTEGER:
	case SQLTypeId::BIGINT:
	case SQLTypeId::DECIMAL:
	case SQLTypeId::FLOAT:
	case SQLTypeId::DOUBLE:
	case SQLTypeId::SQLNULL:
		return true;
	default:
		return false;
	}
}

bool no_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 0;
}

SQLType same_return_type(vector<SQLType> &arguments) {
	assert(arguments.size() == 1);
	return arguments[0];
}

SQLType double_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::DOUBLE;
}

SQLType tint_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::TINYINT;
}

void BuiltinFunctions::RegisterMathFunctions() {
	AddFunction(ScalarFunction("mod", mod_matches_arguments, mod_get_return_type, mod_function));
	AddFunction(ScalarFunction("pow", pow_matches_arguments, pow_get_return_type, pow_function));
	AddFunction(ScalarFunction("power", pow_matches_arguments, pow_get_return_type, pow_function));
	AddFunction(ScalarFunction("round", round_matches_arguments, round_get_return_type, round_function));
	AddFunction(ScalarFunction("abs", single_numeric_argument, same_return_type, abs_function));
	AddFunction(ScalarFunction("cbrt", single_numeric_argument, double_return_type, cbrt_function));
	AddFunction(ScalarFunction("degrees", single_numeric_argument, double_return_type, degrees_function));
	AddFunction(ScalarFunction("radians", single_numeric_argument, double_return_type, radians_function));
	AddFunction(ScalarFunction("exp", single_numeric_argument, double_return_type, exp_function));
	AddFunction(ScalarFunction("ln", single_numeric_argument, double_return_type, ln_function));
	AddFunction(ScalarFunction("log", single_numeric_argument, double_return_type, log10_function));
	AddFunction(ScalarFunction("log10", single_numeric_argument, double_return_type, log10_function));
	AddFunction(ScalarFunction("log2", single_numeric_argument, double_return_type, log2_function));
	AddFunction(ScalarFunction("sqrt", single_numeric_argument, same_return_type, sqrt_function));
	AddFunction(ScalarFunction("ceil", single_numeric_argument, same_return_type, ceil_function));
	AddFunction(ScalarFunction("ceiling", single_numeric_argument, same_return_type, ceil_function));
	AddFunction(ScalarFunction("floor", single_numeric_argument, same_return_type, floor_function));
	AddFunction(ScalarFunction("pi", no_arguments, double_return_type, pi_function));
	AddFunction(ScalarFunction("sign", single_numeric_argument, tint_return_type, sign_function));
}

} // namespace duckdb
