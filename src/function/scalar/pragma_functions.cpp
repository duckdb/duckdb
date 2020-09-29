#include "duckdb/function/pragma/pragma_functions.hpp"

namespace duckdb {
using namespace std;

void BuiltinFunctions::RegisterPragmaFunctions() {
	Register<PragmaQueries>();
	Register<PragmaFunctions>();
}

} // namespace duckdb
