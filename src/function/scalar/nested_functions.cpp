#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<ListValueFun>();
}

} // namespace duckdb
