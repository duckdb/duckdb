#include "rapi.hpp"

using namespace duckdb;

void duckdb::ConnDeleter(ConnWrapper *conn) {
	cpp11::warning("Connection is garbage-collected, use dbDisconnect() to avoid this.");
	delete conn;
}

[[cpp11::register]] duckdb::conn_eptr_t rapi_connect(duckdb::db_eptr_t db) {
	if (!db || !db.get() || !db->db) {
		cpp11::stop("rapi_connect: Invalid database reference");
	}

	auto conn_wrapper = new ConnWrapper();
	conn_wrapper->conn = make_unique<Connection>(*db->db);
	conn_wrapper->db_eptr.swap(db);

	return conn_eptr_t(conn_wrapper);
}

[[cpp11::register]] void rapi_disconnect(duckdb::conn_eptr_t conn) {
	auto conn_wrapper = conn.release();
	if (conn_wrapper) {
		delete conn_wrapper;
	}
}
