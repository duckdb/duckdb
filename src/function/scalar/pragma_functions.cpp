#include "duckdb/function/pragma/pragma_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterPragmaFunctions() {
	Register<PragmaQueries>();
	Register<PragmaFunctions>();
}

} // namespace duckdb
