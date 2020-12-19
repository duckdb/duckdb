#include "duckdb/function/scalar/sequence_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterSequenceFunctions() {
	Register<NextvalFun>();
}

} // namespace duckdb
