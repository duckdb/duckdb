#include "rapi.hpp"

using namespace duckdb;

void duckdb::ConnDeleter(ConnWrapper* conn) {
	Rf_warning(
	    "Connection is garbage-collected, use dbDisconnect() to avoid this.");
  delete conn;
}

conn_eptr_t RApi::Connect(db_eptr_t db) {

	auto db_wrapper = db.get();

	if (!db_wrapper || !db_wrapper->db) {
		cpp11::stop("duckdb_connect_R: Invalid database reference");
	}

	auto conn_wrapper = new ConnWrapper();
	conn_wrapper->db_sexp = db;
	conn_wrapper->conn = make_unique<Connection>(*db_wrapper->db);

	conn_eptr_t conn(conn_wrapper);

	return conn;
}

void RApi::Disconnect(conn_eptr_t conn) {
	auto conn_wrapper = conn.release();
	if (conn_wrapper) {
		delete conn_wrapper;
	}
}
