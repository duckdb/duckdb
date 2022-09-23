#include "odbc_exception.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcException;

const std::string OdbcException::GetComponent() {
	return component;
}

const SQLRETURN OdbcException::GetSqlReturn() {
	return sql_ret;
}

const DiagRecord &OdbcException::GetDiagRecord() {
	return diag_record;
}
