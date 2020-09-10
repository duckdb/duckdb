#include "duckdb/function/table/information_schema_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterInformationSchemaFunctions() {
    InformationSchemaSchemata::RegisterFunction(*this);
}

} // namespace duckdb
