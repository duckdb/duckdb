#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<ReverseFun>();
	Register<LowerFun>();
	Register<UpperFun>();
	Register<StripAccentsFun>();
	Register<ConcatFun>();
	Register<ContainsFun>();
	Register<LengthFun>();
	Register<LikeFun>();
	Register<LikeEscapeFun>();
	Register<LpadFun>();
	Register<LtrimFun>();
	Register<LeftFun>();
	Register<RightFun>();
	Register<PrintfFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<InstrFun>();
	Register<PrefixFun>();
	Register<RepeatFun>();
	Register<ReplaceFun>();
	Register<RpadFun>();
	Register<RtrimFun>();
	Register<SuffixFun>();
	Register<UnicodeFun>();
}

} // namespace duckdb
