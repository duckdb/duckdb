#include "rapi.hpp"
#include "altrepstring.hpp"
#include <R_ext/Rdynload.h>

// When changing this file, run cpp11::cpp_register() from R

[[cpp11::register]] duckdb::db_eptr_t
startup_R(std::string dbdir, bool readonly, cpp11::list config) {
	return duckdb::RApi::Startup(dbdir, readonly, config);
}

[[cpp11::register]] void shutdown_R(duckdb::db_eptr_t db) {
	return duckdb::RApi::Shutdown(db);
}

[[cpp11::register]] duckdb::conn_eptr_t connect_R(duckdb::db_eptr_t db) {
	return duckdb::RApi::Connect(db);
}

[[cpp11::register]] void disconnect_R(duckdb::conn_eptr_t conn) {
	return duckdb::RApi::Disconnect(conn);
}

[[cpp11::register]] cpp11::list prepare_R(duckdb::conn_eptr_t conn,
                                          std::string query) {
	return duckdb::RApi::Prepare(conn, query);
}

[[cpp11::register]] cpp11::list bind_R(cpp11::external_pointer<duckdb::RStatement> stmtsexp, cpp11::list paramsexp,
                                       cpp11::logicals arrowsexp) {
	return duckdb::RApi::Bind(stmtsexp, paramsexp, arrowsexp);
}

[[cpp11::register]] SEXP execute_R(cpp11::external_pointer<duckdb::RStatement> stmtsexp, cpp11::logicals arrowsexp) {
	return duckdb::RApi::Execute(stmtsexp, arrowsexp);
}

[[cpp11::register]] SEXP fetch_arrow_R(SEXP query_resultsexp, cpp11::logicals streamsexp,
                                       cpp11::doubles vector_per_chunksexp, cpp11::logicals return_tablesexp) {
	return duckdb::RApi::DuckDBExecuteArrow(query_resultsexp, streamsexp, vector_per_chunksexp, return_tablesexp);
}

[[cpp11::register]] SEXP fetch_record_batch_R(SEXP query_resultsexp, cpp11::doubles approx_batch_sizeexp) {
	return duckdb::RApi::DuckDBRecordBatchR(query_resultsexp, approx_batch_sizeexp);
}

[[cpp11::register]] void release_R(duckdb::stmt_eptr_t stmt) {
	return duckdb::RApi::Release(stmt);
}

[[cpp11::register]] void register_R(duckdb::conn_eptr_t conn, std::string name, cpp11::data_frame value) {
	return duckdb::RApi::RegisterDataFrame(conn, name, value);
}

[[cpp11::register]] void unregister_R(duckdb::conn_eptr_t conn, std::string name) {
	return duckdb::RApi::UnregisterDataFrame(conn, name);
}

[[cpp11::register]] void register_arrow_R(duckdb::conn_eptr_t connsexp,
                                          cpp11::strings namesexp, cpp11::list export_funsexp, SEXP valuesexp) {
	return duckdb::RApi::RegisterArrow(connsexp, namesexp, export_funsexp, valuesexp);
}

[[cpp11::register]] void unregister_arrow_R(duckdb::conn_eptr_t connsexp,
                                            cpp11::strings namesexp) {
	return duckdb::RApi::UnregisterArrow(connsexp, namesexp);
}

[[cpp11::register]] cpp11::strings ptr_to_str(SEXP extptr) {
	return duckdb::RApi::PointerToString(extptr);
}

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void AltrepString_Initialize(DllInfo* dll) {
	// clang-format on
	AltrepString::Initialize(dll);
}
