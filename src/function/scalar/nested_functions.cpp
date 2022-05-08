#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<ArraySliceFun>();
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	Register<ListAggregateFun>();
	Register<ListDistinctFun>();
	Register<ListUniqueFun>();
	Register<ListValueFun>();
	Register<ListExtractFun>();
	Register<ListSortFun>();
	Register<ListRangeFun>();
	Register<ListFlattenFun>();
	Register<MapFun>();
	Register<MapExtractFun>();
	Register<CardinalityFun>();
}

} // namespace duckdb
