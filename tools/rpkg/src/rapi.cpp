#include "rapi.hpp"
#include "altrepstring.hpp"
using namespace duckdb;

static const R_CallMethodDef R_CallDef[] = {{"duckdb_startup_R", (DL_FUNC)RApi::Startup, 3},
                                            {"duckdb_connect_R", (DL_FUNC)RApi::Connect, 1},
                                            {"duckdb_prepare_R", (DL_FUNC)RApi::Prepare, 2},
                                            {"duckdb_bind_R", (DL_FUNC)RApi::Bind, 2},
                                            {"duckdb_execute_R", (DL_FUNC)RApi::Execute, 2},
                                            {"duckdb_fetch_arrow_R", (DL_FUNC)RApi::DuckDBExecuteArrow, 4},
                                            {"duckdb_fetch_record_batch_R", (DL_FUNC)RApi::DuckDBRecordBatchR, 1},
                                            {"duckdb_release_R", (DL_FUNC)RApi::Release, 1},
                                            {"duckdb_register_R", (DL_FUNC)RApi::RegisterDataFrame, 3},
                                            {"duckdb_unregister_R", (DL_FUNC)RApi::UnregisterDataFrame, 2},
                                            {"duckdb_register_arrow_R", (DL_FUNC)RApi::RegisterArrow, 4},
                                            {"duckdb_unregister_arrow_R", (DL_FUNC)RApi::UnregisterArrow, 2},
                                            {"duckdb_disconnect_R", (DL_FUNC)RApi::Disconnect, 1},
                                            {"duckdb_shutdown_R", (DL_FUNC)RApi::Shutdown, 1},
                                            {"duckdb_ptr_to_str", (DL_FUNC)RApi::PointerToString, 1},
                                            {NULL, NULL, 0}};

extern "C" {
void R_init_duckdb(DllInfo *dll) {
	R_registerRoutines(dll, NULL, R_CallDef, NULL, NULL);
	R_useDynamicSymbols(dll, FALSE);

	AltrepString::Initialize(dll);
	// TODO implement SEXP (*R_altvec_Extract_subset_method_t)(SEXP, SEXP, SEXP);
}
} // extern "C"
