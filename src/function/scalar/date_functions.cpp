#include "function/scalar/date_functions.hpp"

using namespace duckdb;
using namespace std;

void BuiltinFunctions::RegisterDateFunctions() {
	Register<Age>();
	Register<DatePart>();
	Register<Year>();
}
