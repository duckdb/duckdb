#include "cpp11/protect.hpp"

#include "rapi.hpp"

using namespace duckdb;

static SEXP duckdb_finalize_connection_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (conn_wrapper) {
		Rf_warning("duckdb_finalize_connection_R: Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		delete conn_wrapper;
	}
	return R_NilValue;
}

SEXP RApi::Connect(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_connect_R: Need external pointer parameter");
	}
	auto db_wrapper = (DBWrapper *)R_ExternalPtrAddr(dbsexp);
	if (!db_wrapper || !db_wrapper->db) {
		cpp11::stop("duckdb_connect_R: Invalid database reference");
	}

	RProtector r;
	auto conn_wrapper = new ConnWrapper();
	conn_wrapper->db_sexp = dbsexp;
	conn_wrapper->conn = make_unique<Connection>(*db_wrapper->db);

	SEXP connsexp = r.Protect(R_MakeExternalPtr(conn_wrapper, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(connsexp, (void (*)(SEXP))duckdb_finalize_connection_R);

	return connsexp;
}

SEXP RApi::Disconnect(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_disconnect_R: Need external pointer parameter");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (conn_wrapper) {
		R_ClearExternalPtr(connsexp);
		delete conn_wrapper;
	}
	return R_NilValue;
}
