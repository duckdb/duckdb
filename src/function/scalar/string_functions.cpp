#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<LowerFun>();
	Register<UpperFun>();
	Register<StripAccentsFun>();
	Register<ConcatFun>();
	Register<ConcatWSFun>();
	Register<ContainsFun>();
	Register<LengthFun>();
	Register<LikeFun>();
	Register<LikeEscapeFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<PrefixFun>();
	Register<SuffixFun>();
	Register<NFCNormalizeFun>();
	Register<StringSplitFun>();
	Register<StringSplitRegexFun>();
	Register<RegexpEscapeFun>();
	Register<SHA1Fun>();
	Register<SHA256Fun>();
}

} // namespace duckdb
