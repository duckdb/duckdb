#include "duckdb/function/table/information_schema_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterInformationSchemaFunctions() {
	InformationSchemaTables::RegisterFunction(*this);
	InformationSchemaColumns::RegisterFunction(*this);
}

} // namespace duckdb
