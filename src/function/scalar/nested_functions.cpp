#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<StructExtractFun>();
	Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	Register<ListExtractFun>();
	Register<ListResizeFun>();
}

} // namespace duckdb
