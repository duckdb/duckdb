#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<ReverseFun>();
	Register<LowerFun>();
	Register<UpperFun>();
	Register<ConcatFun>();
	Register<LengthFun>();
	Register<LikeFun>();
	Register<LtrimFun>();
	Register<PrintfFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<InstrFun>();
	Register<PrefixFun>();
	Register<ReplaceFun>();
	Register<RtrimFun>();
	Register<SuffixFun>();
	Register<ContainsFun>();
}

} // namespace duckdb
