#include "rapi.hpp"

using namespace duckdb;

void ConnDeleter(ConnWrapper* conn) {
	Rf_warning(
	    "Connection is garbage-collected, use dbDisconnect() to avoid this.");
  delete conn;
}

SEXP RApi::Connect(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_connect_R: Need external pointer parameter");
	}
	auto db_wrapper = (DBWrapper *)R_ExternalPtrAddr(dbsexp);
	if (!db_wrapper || !db_wrapper->db) {
		cpp11::stop("duckdb_connect_R: Invalid database reference");
	}

	auto conn_wrapper = new ConnWrapper();
	conn_wrapper->db_sexp = dbsexp;
	conn_wrapper->conn = make_unique<Connection>(*db_wrapper->db);

	cpp11::external_pointer<ConnWrapper, ConnDeleter> connsexp(conn_wrapper);

	return connsexp;
}

void RApi::Disconnect(SEXP connsexp) {
	if (TYPEOF(connsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_disconnect_R: Need external pointer parameter");
	}
	auto conn_wrapper = (ConnWrapper *)R_ExternalPtrAddr(connsexp);
	if (conn_wrapper) {
		R_ClearExternalPtr(connsexp);
		delete conn_wrapper;
	}
}
