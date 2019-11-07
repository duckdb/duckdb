#include "duckdb/function/scalar/sequence_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSequenceFunctions() {
	Register<NextvalFun>();
}

} // namespace duckdb
