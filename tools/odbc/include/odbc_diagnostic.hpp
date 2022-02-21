#ifndef ODBC_DIAGNOSTIC_HPP
#define ODBC_DIAGNOSTIC_HPP

#include "duckdb.hpp"

#include <sqltypes.h>
#include <string>
#include <vector>

namespace duckdb {
struct DiagRecord {
public:
	std::string sql_diag_class_origin;
	SQLINTEGER sql_diag_column_number;
	std::string sql_diag_connection_name;
	std::string sql_diag_message_text;
	SQLINTEGER sql_diag_native;
	SQLLEN sql_diag_row_number;
	std::string sql_diag_server_name;
	std::string sql_diag_state;
	std::string sql_diag_subclass_origin;
};

struct DiagHeader {
public:
	SQLLEN sql_diag_cursor_row_count;
	std::string sql_diag_dynamic_function;
	SQLINTEGER sql_diag_dynamic_function_code;
	SQLINTEGER sql_diag_number;
	SQLRETURN sql_diag_return_code;
	SQLLEN sql_diag_row_count;
};

class OdbcDiagnostic {
public:
	DiagHeader header;
	std::vector<DiagRecord> diag_records;

	DiagRecord &GetDiagRecord(SQLINTEGER rec_idx);
	// WriteLog
};
} // namespace duckdb
#endif