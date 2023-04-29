#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/function/scalar/uuid_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
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

	// uuid functions
	Register<UUIDFun>();
}

} // namespace duckdb
