#include "duckdb/function/scalar/enum_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterEnumFunctions() {
	Register<EnumFirst>();
	Register<EnumLast>();
	Register<EnumCode>();
	Register<EnumRange>();
	Register<EnumRangeBoundary>();
}

} // namespace duckdb
