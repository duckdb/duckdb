#include "duckdb_odbc.hpp"
#include "odbc_fetch.hpp"
#include "parameter_wrapper.hpp"

using duckdb::OdbcHandleStmt;

OdbcHandleStmt::OdbcHandleStmt(OdbcHandleDbc *dbc_p)
    : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
	D_ASSERT(dbc_p);
	D_ASSERT(dbc_p->conn);

	param_wrapper = make_unique<ParameterWrapper>(&error_messages);
	odbc_fetcher = make_unique<OdbcFetch>();
	dbc->stmt_handle = this;
}

OdbcHandleStmt::~OdbcHandleStmt() {
}

SQLRETURN OdbcHandleStmt::MaterializeResult() {
	if (!stmt || !stmt->success) {
		return SQL_SUCCESS;
	}
	if (!res || !res->success) {
		return SQL_SUCCESS;
	}
	return odbc_fetcher->Materialize(this);
}
