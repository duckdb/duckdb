#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<ArraySliceFun>();
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<ListValueFun>();
	Register<ListExtractFun>();
	Register<MapFun>();
	Register<MapExtractFun>();
	Register<CardinalityFun>();
}

} // namespace duckdb
