#include "duckdb/function/scalar/date_functions.hpp"

using namespace duckdb;
using namespace std;

void BuiltinFunctions::RegisterDateFunctions() {
	Register<AgeFun>();
	Register<DatePartFun>();
	Register<DateTruncFun>();
	Register<CurrentTimeFun>();
	Register<CurrentDateFun>();
	Register<CurrentTimestampFun>();
	Register<EpochFun>();
}
