#include "rapi.hpp"

using namespace duckdb;

namespace duckdb {

void ConnDeleter(ConnWrapper* conn) {
	Rf_warning(
	    "Connection is garbage-collected, use dbDisconnect() to avoid this.");
  delete conn;
}

}

conn_eptr_t RApi::Connect(db_eptr_t dbsexp) {

	auto db_wrapper = dbsexp.get();

	if (!db_wrapper || !db_wrapper->db) {
		cpp11::stop("duckdb_connect_R: Invalid database reference");
	}

	auto conn_wrapper = new ConnWrapper();
	conn_wrapper->db_sexp = dbsexp;
	conn_wrapper->conn = make_unique<Connection>(*db_wrapper->db);

	conn_eptr_t connsexp(conn_wrapper);

	return connsexp;
}

void RApi::Disconnect(conn_eptr_t connsexp) {
	auto conn_wrapper = connsexp.release();
	if (conn_wrapper) {
		delete conn_wrapper;
	}
}
