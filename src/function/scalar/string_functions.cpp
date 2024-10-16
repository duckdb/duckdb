#include "duckdb/function/scalar/string_functions_tmp.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<SHA1Fun>();
	Register<SHA256Fun>();
	Register<MD5Fun>();
	Register<MD5NumberFun>();
}

} // namespace duckdb
