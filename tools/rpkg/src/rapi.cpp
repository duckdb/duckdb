#include "rapi.hpp"
#include "altrepstring.hpp"
#include <R_ext/Rdynload.h>

// When changing this file, run cpp11::cpp_register() from R

[[cpp11::register]] SEXP fetch_arrow_R(SEXP query_resultsexp, cpp11::logicals streamsexp,
                                       cpp11::doubles vector_per_chunksexp, cpp11::logicals return_tablesexp) {
	return duckdb::DuckDBExecuteArrow(query_resultsexp, streamsexp, vector_per_chunksexp, return_tablesexp);
}

[[cpp11::register]] SEXP fetch_record_batch_R(SEXP query_resultsexp, cpp11::doubles approx_batch_sizeexp) {
	return duckdb::DuckDBRecordBatchR(query_resultsexp, approx_batch_sizeexp);
}

[[cpp11::register]] cpp11::strings ptr_to_str(SEXP extptr) {
	return duckdb::PointerToString(extptr);
}

// exception required as long as r-lib/decor#6 remains
// clang-format off
[[cpp11::init]] void AltrepString_Initialize(DllInfo* dll) {
	// clang-format on
	AltrepString::Initialize(dll);
}
