#include "duckdb/function/scalar/date_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterDateFunctions() {
	Register<AgeFun>();
	Register<DateDiffFun>();
	Register<DatePartFun>();
	Register<DateSubFun>();
	Register<DateTruncFun>();
	Register<CurrentTimeFun>();
	Register<CurrentDateFun>();
	Register<CurrentTimestampFun>();
	Register<EpochFun>();
	Register<MakeDateFun>();
	Register<StrfTimeFun>();
	Register<StrpTimeFun>();
	Register<ToIntervalFun>();
}

} // namespace duckdb
