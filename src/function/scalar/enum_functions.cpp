#include "duckdb/function/scalar/enum_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterEnumFunctions() {
	Register<EnumFirst>();
}

} // namespace duckdb
