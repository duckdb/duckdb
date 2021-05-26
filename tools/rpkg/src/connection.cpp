#include "duckdbr.hpp"

using namespace duckdb;

static SEXP duckdb_finalize_connection_R(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	Connection *connaddr = (Connection *)R_ExternalPtrAddr(connsexp);
	if (connaddr) {
		Rf_warning("duckdb_finalize_connection_R: Connection is garbage-collected, use dbDisconnect() to avoid this.");
		R_ClearExternalPtr(connsexp);
		delete connaddr;
	}
	return R_NilValue;
}

SEXP RApi::Connect(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_connect_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (!dbaddr) {
		Rf_error("duckdb_connect_R: Invalid database reference");
	}

	RProtector r;
	SEXP connsexp = r.Protect(R_MakeExternalPtr(new Connection(*dbaddr), R_NilValue, R_NilValue));
	R_RegisterCFinalizer(connsexp, (void (*)(SEXP))duckdb_finalize_connection_R);

	return connsexp;
}

SEXP RApi::Disconnect(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		Rf_error("duckdb_disconnect_R: Need external pointer parameter");
	}
	Connection *connaddr = (Connection *)R_ExternalPtrAddr(connsexp);
	if (connaddr) {
		R_ClearExternalPtr(connsexp);
		delete connaddr;
	}
	return R_NilValue;
}
