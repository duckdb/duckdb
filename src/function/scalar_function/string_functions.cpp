#include "function/scalar_function/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	AddFunction(ScalarFunction("upper", caseconvert_matches_arguments, caseconvert_get_return_type, caseconvert_upper_function));
	AddFunction(ScalarFunction("lower", caseconvert_matches_arguments, caseconvert_get_return_type, caseconvert_lower_function));
	AddFunction(ScalarFunction("concat", concat_matches_arguments, concat_get_return_type, concat_function));
	AddFunction(ScalarFunction("length", length_matches_arguments, length_get_return_type, length_function));
	AddFunction(ScalarFunction("regexp_matches", regexp_matches_matches_arguments, regexp_matches_get_return_type, regexp_matches_function, false, regexp_matches_get_bind_function));
	AddFunction(ScalarFunction("regexp_replace", regexp_replace_matches_arguments, regexp_replace_get_return_type, regexp_replace_function));
	AddFunction(ScalarFunction("substring", substring_matches_arguments, substring_get_return_type, substring_function));
}

}
