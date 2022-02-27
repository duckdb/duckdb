#include "odbc_exception.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcException;

// OdbcException::OdbcException(const std::string &msg) {
//     diag_record.sql_diag_message_text = msg;
// }

// OdbcException::OdbcException(const std::string &msg, const std::string &sqlstate, const std::string &server_name,
// SQLINTEGER col_number, SQLINTEGER sql_native, SQLLEN row_number) {
//     diag_record.sql_diag_message_text = msg;
//    	diag_record.sql_diag_sqlstate = sqlstate;
//    	diag_record.sql_diag_server_name = server_name;
//     diag_record.sql_diag_column_number = col_number;
// 	diag_record.sql_diag_native = sql_native;
// 	diag_record.sql_diag_row_number = row_number;
// }

// void OdbcException::SetMessage(const std::string &message) {
//     diag_record.sql_diag_message_text = message;
// }

// void OdbcException::SetComponent(const std::string &comp) {
//     component = comp;
// }

// void OdbcException::SetSqlReturn(SQLRETURN ret) {
//     sql_ret = ret;
// }

// const std::string OdbcException::GetMessage() {
//     return diag_record.sql_diag_message_text;
// }

const std::string OdbcException::GetComponent() {
	return component;
}

const SQLRETURN OdbcException::GetSqlReturn() {
	return sql_ret;
}

const DiagRecord OdbcException::GetDiagRecord() {
	return diag_record;
}