#include "duckdb/function/table/sqlite_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	AddFunction(TableFunction(
	    "sqlite_master", {}, {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR, SQLType::INTEGER, SQLType::VARCHAR},
	    {"type", "name", "tbl_name", "rootpage", "sql"}, sqlite_master_init, sqlite_master, nullptr));

	AddFunction(TableFunction("pragma_table_info", {SQLType::VARCHAR},
	                          {SQLType::INTEGER, SQLType::VARCHAR, SQLType::VARCHAR, SQLType(SQLTypeId::BOOLEAN),
	                           SQLType::VARCHAR, SQLType(SQLTypeId::BOOLEAN)},
	                          {"cid", "name", "type", "notnull", "dflt_value", "pk"}, pragma_table_info_init,
	                          pragma_table_info, nullptr));
}

} // namespace duckdb
