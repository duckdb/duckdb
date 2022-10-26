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

	const std::string GetComponent();
	const SQLRETURN GetSqlReturn();
	const DiagRecord &GetDiagRecord();

private:
	std::string component;
	SQLRETURN sql_ret = SQL_ERROR;
	duckdb::DiagRecord diag_record;
};
} // namespace duckdb
#endif
