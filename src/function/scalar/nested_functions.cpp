#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<ArraySliceFun>();
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<StructInsertFun>();
	Register<ListConcatFun>();
	Register<ListContainsFun>();
	Register<ListPositionFun>();
	Register<ListAggregateFun>();
	Register<ListDistinctFun>();
	Register<ListUniqueFun>();
	Register<ListExtractFun>();
	Register<ListRangeFun>();
	Register<MapFun>();
	Register<MapFromEntriesFun>();
	Register<MapEntriesFun>();
	Register<MapValuesFun>();
	Register<MapKeysFun>();
	Register<MapExtractFun>();
	Register<UnionValueFun>();
	Register<UnionExtractFun>();
	Register<UnionTagFun>();
	Register<CardinalityFun>();
}

} // namespace duckdb
