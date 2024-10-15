#include "duckdb/function/scalar/string_functions_tmp.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<LikeFun>();
	Register<LikeEscapeFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<StringSplitFun>();
	Register<StringSplitRegexFun>();
	Register<RegexpEscapeFun>();
	Register<SHA1Fun>();
	Register<SHA256Fun>();
	Register<MD5Fun>();
	Register<MD5NumberFun>();
}

} // namespace duckdb
