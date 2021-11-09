#include "duckdb_odbc.hpp"
#include "odbc_fetch.hpp"
#include "parameter_wrapper.hpp"

using duckdb::OdbcHandleDbc;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcHandleStmt;

//! OdbcHandleDbc functions ***************************************************
OdbcHandleDbc::~OdbcHandleDbc() {
	// this is needed because some applications may not call SQLFreeHandle
	for (auto stmt : vec_stmt_ref) {
		delete stmt;
	}
}

void OdbcHandleDbc::EraseStmtRef(OdbcHandleStmt *stmt) {
	// erase the reference from vec_stmt_ref
	for (duckdb::idx_t v_idx = 0; v_idx < vec_stmt_ref.size(); ++v_idx) {
		if (vec_stmt_ref[v_idx] == stmt) {
			vec_stmt_ref.erase(vec_stmt_ref.begin() + v_idx);
			break;
		}
	}
}

SQLRETURN OdbcHandleDbc::MaterializeResult() {
	if (vec_stmt_ref.empty()) {
		return SQL_SUCCESS;
	}
	// only materializing the result set from the last statement
	return vec_stmt_ref.back()->MaterializeResult();
}

//! OdbcHandleStmt functions **************************************************
OdbcHandleStmt::OdbcHandleStmt(OdbcHandleDbc *dbc_p)
    : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
	D_ASSERT(dbc_p);
	D_ASSERT(dbc_p->conn);

	param_wrapper = make_unique<ParameterWrapper>(&error_messages);
	odbc_fetcher = make_unique<OdbcFetch>();
	dbc->vec_stmt_ref.emplace_back(this);

	apd = make_unique<OdbcHandleDesc>();
	ipd = make_unique<OdbcHandleDesc>();
	ard = make_unique<OdbcHandleDesc>();
	ird = make_unique<OdbcHandleDesc>();
}

OdbcHandleStmt::~OdbcHandleStmt() {
}

void OdbcHandleStmt::Close() {
	open = false;
	res.reset();
	odbc_fetcher->ClearChunks();
	param_wrapper->Reset();
	// stmt->stmt.reset(); // the statment can be reuse in prepared statement
	bound_cols.clear();
	// stmt->param_wrapper->Clear(); // the parameter values can be reused after
	error_messages.clear();
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
