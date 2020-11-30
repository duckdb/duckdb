#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {
using namespace std;

void BuiltinFunctions::RegisterGenericFunctions() {
	Register<LeastFun>();
	Register<GreatestFun>();
	Register<StatsFun>();
}

} // namespace duckdb
