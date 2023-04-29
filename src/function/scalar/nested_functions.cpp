#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<StructExtractFun>();
	Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	Register<ListExtractFun>();
	Register<UnionValueFun>();
	Register<UnionExtractFun>();
	Register<UnionTagFun>();
}

} // namespace duckdb
