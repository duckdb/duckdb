#include "duckdb_odbc.hpp"
#include "odbc_fetch.hpp"

// duckdb::OdbcHandle::~OdbcHandle() = default;

duckdb::OdbcHandleStmt::OdbcHandleStmt(OdbcHandleDbc *dbc_p)
    : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr), row_count(0) {
	D_ASSERT(dbc_p);
	D_ASSERT(dbc_p->conn);

	odbc_fetcher = make_unique<OdbcFetch>();
}

duckdb::OdbcHandleStmt::~OdbcHandleStmt() {}
