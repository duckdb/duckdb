#include "cpp11.hpp"
#include "rapi.hpp"
#include "altrepstring.hpp"
#include <R_ext/Rdynload.h>

using namespace duckdb;

// When changing this file, run cpp11::cpp_register() from R

[[cpp11::register]]
cpp11::external_pointer<DBWrapper> startup_R(cpp11::strings dbdirsexp, cpp11::logicals readonlysexp, cpp11::list configsexp) {
	return RApi::Startup(dbdirsexp, readonlysexp, configsexp);
}

[[cpp11::register]]
SEXP shutdown_R(SEXP dbsexp) {
	return RApi::Shutdown(dbsexp);
}

[[cpp11::register]]
SEXP connect_R(SEXP dbsexp) {
	return RApi::Connect(dbsexp);
}

[[cpp11::register]]
SEXP disconnect_R(SEXP connsexp) {
	return RApi::Disconnect(connsexp);
}

[[cpp11::register]]
SEXP prepare_R(SEXP connsexp, SEXP querysexp) {
	return RApi::Prepare(connsexp, querysexp);
}

[[cpp11::register]]
SEXP bind_R(SEXP stmtsexp, SEXP paramsexp, SEXP arrowsexp) {
	return RApi::Bind(stmtsexp, paramsexp, arrowsexp);
}

[[cpp11::register]]
SEXP execute_R(SEXP stmtsexp, SEXP arrowsexp) {
	return RApi::Execute(stmtsexp, arrowsexp);
}

[[cpp11::register]]
SEXP fetch_arrow_R(SEXP query_resultsexp, SEXP streamsexp, SEXP vector_per_chunksexp, SEXP return_tablesexp) {
	return RApi::DuckDBExecuteArrow(query_resultsexp, streamsexp, vector_per_chunksexp, return_tablesexp);
}

[[cpp11::register]]
SEXP fetch_record_batch_R(SEXP query_resultsexp, SEXP approx_batch_sizeexp) {
	return RApi::DuckDBRecordBatchR(query_resultsexp, approx_batch_sizeexp);
}

[[cpp11::register]]
SEXP release_R(SEXP stmtsexp) {
	return RApi::Release(stmtsexp);
}

[[cpp11::register]]
SEXP register_R(SEXP connsexp, SEXP namesexp, SEXP valuesexp) {
	return RApi::RegisterDataFrame(connsexp, namesexp, valuesexp);
}

[[cpp11::register]]
SEXP unregister_R(SEXP connsexp, SEXP namesexp) {
	return RApi::UnregisterDataFrame(connsexp, namesexp);
}

[[cpp11::register]]
SEXP register_arrow_R(SEXP connsexp, SEXP namesexp, SEXP export_funsexp, SEXP valuesexp) {
	return RApi::RegisterArrow(connsexp, namesexp, export_funsexp, valuesexp);
}

[[cpp11::register]]
SEXP unregister_arrow_R(SEXP connsexp, SEXP namesexp) {
	return RApi::UnregisterArrow(connsexp, namesexp);
}

[[cpp11::register]]
SEXP ptr_to_str(SEXP extptr) {
	return RApi::PointerToString(extptr);
}

[[cpp11::init]]
void AltrepString_Initialize(DllInfo* dll) {
	AltrepString::Initialize(dll);
}
