#include "duckdb_odbc.hpp"
#include "odbc_fetch.hpp"
#include "odbc_interval.hpp"
#include "descriptor.hpp"
#include "parameter_descriptor.hpp"
#include "row_descriptor.hpp"

using duckdb::OdbcHandleDbc;
using duckdb::OdbcHandleDesc;
using duckdb::OdbcHandleStmt;
using duckdb::OdbcHandleType;

std::string duckdb::OdbcHandleTypeToString(OdbcHandleType type) {
	switch (type) {
	case OdbcHandleType::ENV:
		return "ENV";
	case OdbcHandleType::DBC:
		return "DBC";
	case OdbcHandleType::STMT:
		return "STMT";
	case OdbcHandleType::DESC:
		return "DESC";
	}
	return "INVALID";
}

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

void OdbcHandleDbc::ResetStmtDescriptors(OdbcHandleDesc *old_desc) {
	for (auto stmt : vec_stmt_ref) {
		if (stmt->param_desc->GetAPD() == old_desc) {
			stmt->param_desc->ResetCurrentAPD();
		}
		if (stmt->row_desc->GetARD() == old_desc) {
			stmt->row_desc->ResetCurrentARD();
		}
	}
}

//! OdbcHandleStmt functions **************************************************
OdbcHandleStmt::OdbcHandleStmt(OdbcHandleDbc *dbc_p)
    : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
	D_ASSERT(dbc_p);
	D_ASSERT(dbc_p->conn);

	odbc_fetcher = make_unique<OdbcFetch>();
	dbc->vec_stmt_ref.emplace_back(this);

	// Implicit parameter and row descriptor associated with this ODBC handle statement
	param_desc = make_unique<ParameterDescriptor>(this);
	row_desc = make_unique<RowDescriptor>(this);
}

OdbcHandleStmt::~OdbcHandleStmt() {
}

void OdbcHandleStmt::Close() {
	open = false;
	res.reset();
	odbc_fetcher->ClearChunks();
	// the parameter values can be reused after
	param_desc->Reset();
	// stmt->stmt.reset(); // the statment can be reuse in prepared statement
	bound_cols.clear();
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

void OdbcHandleStmt::SetARD(OdbcHandleDesc *new_ard) {
	row_desc->SetCurrentARD(new_ard);
}

void OdbcHandleStmt::SetAPD(OdbcHandleDesc *new_apd) {
	param_desc->SetCurrentAPD(new_apd);
}
