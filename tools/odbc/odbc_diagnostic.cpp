#include "odbc_diagnostic.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcDiagnostic;
using std::string;

const std::unordered_map<SQLINTEGER, std::string> OdbcDiagnostic::map_dynamic_function = {
    {SQL_DIAG_ALTER_DOMAIN, "ALTER DOMAIN"},
    {SQL_DIAG_ALTER_TABLE, "ALTER TABLE"},
    {SQL_DIAG_CREATE_ASSERTION, "CREATE ASSERTION"},
    {SQL_DIAG_CREATE_CHARACTER_SET, "CREATE CHARACTER SET"},
    {SQL_DIAG_CREATE_COLLATION, "CREATE COLLATION"},
    {SQL_DIAG_CREATE_DOMAIN, "CREATE DOMAIN"},
    {SQL_DIAG_CREATE_INDEX, "CREATE INDEX"},
    {SQL_DIAG_CREATE_TABLE, "CREATE TABLE"},
    {SQL_DIAG_CREATE_VIEW, "CREATE VIEW"},
    {SQL_DIAG_SELECT_CURSOR, "SELECT CURSOR"},
    {SQL_DIAG_DYNAMIC_DELETE_CURSOR, "DYNAMIC DELETE CURSOR"},
    {SQL_DIAG_DELETE_WHERE, "DELETE WHERE"},
    {SQL_DIAG_DROP_ASSERTION, "DROP ASSERTION"},
    {SQL_DIAG_DROP_CHARACTER_SET, "DROP CHARACTER SET"},
    {SQL_DIAG_DROP_COLLATION, "DROP COLLATION"},
    {SQL_DIAG_DROP_DOMAIN, "DROP DOMAIN"},
    {SQL_DIAG_DROP_INDEX, "DROP INDEX"},
    {SQL_DIAG_DROP_SCHEMA, "DROP SCHEMA"},
    {SQL_DIAG_DROP_TABLE, "DROP TABLE"},
    {SQL_DIAG_DROP_TRANSLATION, "DROP TRANSLATION"},
    {SQL_DIAG_DROP_VIEW, "DROP VIEW"},
    {SQL_DIAG_GRANT, "GRANT"},
    {SQL_DIAG_INSERT, "INSERT"},
    {SQL_DIAG_CALL, "CALL"},
    {SQL_DIAG_REVOKE, "REVOKE"},
    {SQL_DIAG_CREATE_SCHEMA, "CREATE SCHEMA"},
    {SQL_DIAG_CREATE_TRANSLATION, "CREATE TRANSLATION"},
    {SQL_DIAG_DYNAMIC_UPDATE_CURSOR, "DYNAMIC UPDATE CURSOR"},
    {SQL_DIAG_UPDATE_WHERE, "UPDATE WHERE"},
    {SQL_DIAG_UNKNOWN_STATEMENT, ""}};

const std::set<std::string> OdbcDiagnostic::set_odbc3_subclass_origin = {
    {"01S00"}, {"01S01"}, {"01S02"}, {"01S06"}, {"01S07"}, {"07S01"}, {"08S01"}, {"21S01"}, {"21S02"},
    {"25S01"}, {"25S02"}, {"25S03"}, {"42S01"}, {"42S02"}, {"42S11"}, {"42S12"}, {"42S21"}, {"42S22"},
    {"HY095"}, {"HY097"}, {"HY098"}, {"HY099"}, {"HY100"}, {"HY101"}, {"HY105"}, {"HY107"}, {"HY109"},
    {"HY110"}, {"HY111"}, {"HYT00"}, {"HYT01"}, {"IM001"}, {"IM002"}, {"IM003"}, {"IM004"}, {"IM005"},
    {"IM006"}, {"IM007"}, {"IM008"}, {"IM010"}, {"IM011"}, {"IM012"}};

bool OdbcDiagnostic::IsDiagRecordField(SQLSMALLINT rec_field) {
	switch (rec_field) {
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_COLUMN_NUMBER:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_NATIVE:
	case SQL_DIAG_ROW_NUMBER:
	case SQL_DIAG_SERVER_NAME:
	case SQL_DIAG_SQLSTATE:
	case SQL_DIAG_SUBCLASS_ORIGIN:
		return true;
	default:
		return false;
	}
}

string OdbcDiagnostic::GetDiagDynamicFunction() {
	auto entry = map_dynamic_function.find(header.sql_diag_dynamic_function_code);
	if (entry == map_dynamic_function.end()) {
		return "";
	}
	return entry->second;
}

bool OdbcDiagnostic::VerifyRecordIndex(SQLINTEGER rec_idx) {
	return (rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
}

const DiagRecord &OdbcDiagnostic::GetDiagRecord(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	return diag_records[rec_idx];
}

std::string OdbcDiagnostic::GetDiagClassOrigin(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	auto sqlstate_str = diag_records[rec_idx].sql_diag_sqlstate;
	if (sqlstate_str.find("IM") != std::string::npos) {
		return "ODBC 3.0";
	} else {
		return "ISO 9075";
	}
}

std::string OdbcDiagnostic::GetDiagSubclassOrigin(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	auto sqlstate_str = diag_records[rec_idx].sql_diag_sqlstate;
	if (set_odbc3_subclass_origin.find(sqlstate_str) != set_odbc3_subclass_origin.end()) {
		return "ODBC 3.0";
	} else {
		return "ISO 9075";
	}
}