#include "duckdb/function/scalar/debug_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterDebugFunctions() {
	Register<VectorTypeFun>();
}

} // namespace duckdb
