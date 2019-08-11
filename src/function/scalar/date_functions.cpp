#include "function/scalar/date_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterDateFunctions() {
	AddFunction(ScalarFunction("age", age_matches_arguments, age_get_return_type, age_function));
	AddFunction(ScalarFunction("date_part", date_part_matches_arguments, date_part_get_return_type, date_part_function));
	AddFunction(ScalarFunction("year", year_matches_arguments, year_get_return_type, year_function));
}

}
