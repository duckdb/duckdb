#include "odbc_diagnostic.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcDiagnostic;

DiagRecord &OdbcDiagnostic::GetDiagRecord(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	return diag_records[rec_idx];
}

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