#ifndef ODBC_DIAGNOSTIC_HPP
#define ODBC_DIAGNOSTIC_HPP

#include "duckdb.hpp"

#include "sqlext.h"
#include "sqltypes.h"

#include <set>
#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {
struct DiagRecord {
public:
	// Some fields were commented out because they can be extract from other fields or internal data structures
	// std::string sql_diag_class_origin;
	SQLINTEGER sql_diag_column_number = SQL_NO_COLUMN_NUMBER;
	// std::string sql_diag_connection_name;
	std::string sql_diag_message_text;
	SQLINTEGER sql_diag_native = 0;
	SQLLEN sql_diag_row_number = SQL_NO_ROW_NUMBER;
	std::string sql_diag_server_name;
	std::string sql_diag_sqlstate;
	// std::string sql_diag_subclass_origin;
};

struct DiagHeader {
public:
	SQLLEN sql_diag_cursor_row_count;
	// std::string sql_diag_dynamic_function; // this field is extract from map_dynamic_function
	SQLINTEGER sql_diag_dynamic_function_code = SQL_DIAG_UNKNOWN_STATEMENT;
	SQLINTEGER sql_diag_number;
	SQLRETURN sql_diag_return_code;
	SQLLEN sql_diag_row_count;
};

class OdbcDiagnostic {
public:
	DiagHeader header;
	std::vector<DiagRecord> diag_records;
	static const std::unordered_map<SQLINTEGER, std::string> MAP_DYNAMIC_FUNCTION;
	static const std::set<std::string> SET_ODBC3_SUBCLASS_ORIGIN;
	static const std::unordered_map<std::string, std::string> MAP_ODBC_SQL_STATES;

public:
	static bool IsDiagRecordField(SQLSMALLINT diag_identifier);

	void FormatDiagnosticMessage(DiagRecord &diag_record, const std::string &data_source, const std::string &component);
	void AddDiagRecord(const DiagRecord &diag_record);

	std::string GetDiagDynamicFunction();
	bool VerifyRecordIndex(SQLINTEGER rec_idx);
	const DiagRecord &GetDiagRecord(SQLINTEGER rec_idx);
	std::string GetDiagClassOrigin(SQLINTEGER rec_idx);
	std::string GetDiagSubclassOrigin(SQLINTEGER rec_idx);
	// WriteDiagnostic
	// newODBCError(const char *SQLState, const char *msg, int nativeCode)
};
} // namespace duckdb
#endif