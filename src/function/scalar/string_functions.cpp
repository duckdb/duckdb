#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/uuid_functions.hpp"

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
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<PrefixFun>();
	Register<SuffixFun>();
	Register<NFCNormalizeFun>();
	Register<StringSplitFun>();

	// uuid functions
	Register<UUIDFun>();
}

} // namespace duckdb
