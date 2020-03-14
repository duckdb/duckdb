#include "duckdb/function/scalar/nested_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterNestedFunctions() {
	Register<StructPackFun>();
	Register<StructExtractFun>();
	Register<ListValueFun>();
}

} // namespace duckdb
