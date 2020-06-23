#include "duckdb/function/scalar/generic_functions.hpp"

using namespace duckdb;
using namespace std;

void BuiltinFunctions::RegisterGenericFunctions() {
	Register<LeastFun>();
	Register<GreatestFun>();
}
