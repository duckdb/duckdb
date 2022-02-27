#ifndef ODBC_EXCEPTION_HPP
#define ODBC_EXCEPTION_HPP

#include "odbc_diagnostic.hpp"
#include "sqlext.h"
#include "sqltypes.h"

#include <stdexcept>
#include <string>

namespace duckdb {
struct OdbcException : public std::exception {
public:
	explicit OdbcException(const std::string &comp, SQLRETURN ret, const DiagRecord &diag_rec)
	    : component(comp), sql_ret(ret), diag_record(diag_rec) {};

	// explicit OdbcException(const std::string &msg, const std::string &sqlstate="", const std::string &server_name="",
	// SQLINTEGER col_number=SQL_NO_COLUMN_NUMBER, SQLINTEGER sql_native=0, SQLLEN row_number=SQL_NO_ROW_NUMBER);
	// explicit OdbcException(const duckdb::DiagRecord &diag_rec): diag_record(diag_rec) {
	// };

	// void SetComponent(const std::string &comp);
	// void SetSqlReturn(SQLRETURN ret);
	// void SetMessage(const std::string &message);

	// const std::string GetMessage();
	const std::string GetComponent();
	const SQLRETURN GetSqlReturn();
	const DiagRecord GetDiagRecord();

private:
	std::string component;
	SQLRETURN sql_ret = SQL_ERROR;
	duckdb::DiagRecord diag_record;
};
} // namespace duckdb
#endif