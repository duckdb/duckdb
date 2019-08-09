#include "function/table_function/sqlite_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	AddFunction(TableFunction("sqlite_master", {}, {
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::INTEGER),
		SQLType(SQLTypeId::VARCHAR)}, {
		"type",
		"name",
		"tbl_name",
		"rootpage",
		"sql"
	}, sqlite_master_init, sqlite_master, nullptr));

	AddFunction(TableFunction("pragma_table_info",
		{ SQLType(SQLTypeId::VARCHAR) }, {
		SQLType(SQLTypeId::INTEGER),
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::BOOLEAN),
		SQLType(SQLTypeId::VARCHAR),
		SQLType(SQLTypeId::BOOLEAN)
	}, {
		"cid",
		"name",
		"type",
		"notnull",
		"dflt_value",
		"pk"
	}, pragma_table_info_init, pragma_table_info, nullptr));
}

}
