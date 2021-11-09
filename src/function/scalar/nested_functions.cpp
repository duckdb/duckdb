#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<ArraySliceFun>();
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<ListConcatFun>();
	Register<ListValueFun>();
	Register<ListExtractFun>();
	Register<ListRangeFun>();
	Register<MapFun>();
	Register<MapExtractFun>();
	Register<CardinalityFun>();
}

} // namespace duckdb
