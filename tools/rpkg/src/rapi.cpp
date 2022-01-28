#include "rapi.hpp"
#include "altrepstring.hpp"
#include <R_ext/Rdynload.h>

using namespace duckdb;

// When changing this file, run cpp11::cpp_register() from R

[[cpp11::register]] cpp11::external_pointer<duckdb::DBWrapper>
startup_R(cpp11::strings dbdirsexp, cpp11::logicals readonlysexp, cpp11::list configsexp) {
	return RApi::Startup(dbdirsexp, readonlysexp, configsexp);
}

[[cpp11::register]] void shutdown_R(cpp11::external_pointer<duckdb::DBWrapper> dbsexp) {
	return RApi::Shutdown(dbsexp);
}

[[cpp11::register]] cpp11::external_pointer<duckdb::ConnWrapper>
connect_R(cpp11::external_pointer<duckdb::DBWrapper> dbsexp) {
	return RApi::Connect(dbsexp);
}

[[cpp11::register]] void disconnect_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp) {
	return RApi::Disconnect(connsexp);
}

[[cpp11::register]] cpp11::list prepare_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp,
                                          cpp11::strings querysexp) {
	return RApi::Prepare(connsexp, querysexp);
}

[[cpp11::register]] cpp11::list bind_R(cpp11::external_pointer<duckdb::RStatement> stmtsexp, cpp11::list paramsexp,
                                       cpp11::logicals arrowsexp) {
	return RApi::Bind(stmtsexp, paramsexp, arrowsexp);
}

[[cpp11::register]] SEXP execute_R(cpp11::external_pointer<duckdb::RStatement> stmtsexp, cpp11::logicals arrowsexp) {
	return RApi::Execute(stmtsexp, arrowsexp);
}

[[cpp11::register]] SEXP fetch_arrow_R(SEXP query_resultsexp, cpp11::logicals streamsexp,
                                       cpp11::doubles vector_per_chunksexp, cpp11::logicals return_tablesexp) {
	return RApi::DuckDBExecuteArrow(query_resultsexp, streamsexp, vector_per_chunksexp, return_tablesexp);
}

[[cpp11::register]] SEXP fetch_record_batch_R(SEXP query_resultsexp, cpp11::doubles approx_batch_sizeexp) {
	return RApi::DuckDBRecordBatchR(query_resultsexp, approx_batch_sizeexp);
}

[[cpp11::register]] SEXP release_R(cpp11::external_pointer<duckdb::RStatement> stmtsexp) {
	return RApi::Release(stmtsexp);
}

[[cpp11::register]] void register_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp, cpp11::strings namesexp,
                                    cpp11::list valuesexp) {
	return RApi::RegisterDataFrame(connsexp, namesexp, valuesexp);
}

[[cpp11::register]] void unregister_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp, cpp11::strings namesexp) {
	return RApi::UnregisterDataFrame(connsexp, namesexp);
}

[[cpp11::register]] void register_arrow_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp,
                                          cpp11::strings namesexp, cpp11::list export_funsexp, SEXP valuesexp) {
	return RApi::RegisterArrow(connsexp, namesexp, export_funsexp, valuesexp);
}

[[cpp11::register]] void unregister_arrow_R(cpp11::external_pointer<duckdb::ConnWrapper> connsexp,
                                            cpp11::strings namesexp) {
	return RApi::UnregisterArrow(connsexp, namesexp);
}

[[cpp11::register]] cpp11::strings ptr_to_str(SEXP extptr) {
	return RApi::PointerToString(extptr);
}

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void AltrepString_Initialize(DllInfo* dll) {
	// clang-format on
	AltrepString::Initialize(dll);
}