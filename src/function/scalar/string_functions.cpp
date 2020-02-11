#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<LowerFun>();
	Register<UpperFun>();
	Register<ConcatFun>();
	Register<LengthFun>();
	Register<LikeFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
}

} // namespace duckdb
