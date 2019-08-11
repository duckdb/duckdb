#include "function/scalar/trigonometric_functions.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "common/exception.hpp"

using namespace std;

namespace duckdb {

bool trig_matches_arguments(vector<SQLType> &arguments) {
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

SQLType trig_get_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::DOUBLE;
}

void BuiltinFunctions::RegisterTrigonometricsFunctions() {
	AddFunction(ScalarFunction("sin", trig_matches_arguments, trig_get_return_type, sin_function));
	AddFunction(ScalarFunction("cos", trig_matches_arguments, trig_get_return_type, cos_function));
	AddFunction(ScalarFunction("tan", trig_matches_arguments, trig_get_return_type, tan_function));
	AddFunction(ScalarFunction("asin", trig_matches_arguments, trig_get_return_type, asin_function));
	AddFunction(ScalarFunction("acos", trig_matches_arguments, trig_get_return_type, acos_function));
	AddFunction(ScalarFunction("atan", trig_matches_arguments, trig_get_return_type, atan_function));
	AddFunction(ScalarFunction("cot", trig_matches_arguments, trig_get_return_type, cot_function));
	AddFunction(ScalarFunction("atan2", atan2_matches_arguments, trig_get_return_type, atan2_function));
}

} // namespace duckdb
