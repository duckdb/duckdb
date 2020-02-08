#include "duckdb/function/scalar/struct_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStructFunctions() {
	Register<StructPackFun>();
	Register<StructExtractFun>();
}

} // namespace duckdb
