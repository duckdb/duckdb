#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterGenericFunctions() {
	Register<AliasFun>();
	Register<HashFun>();
	Register<LeastFun>();
	Register<GreatestFun>();
	Register<StatsFun>();
	Register<TypeOfFun>();
	Register<CurrentSettingFun>();
	Register<SystemFun>();
}

} // namespace duckdb
