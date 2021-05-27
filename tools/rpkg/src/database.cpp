#include "rapi.hpp"
#include "duckdb/main/client_context.hpp"
#include "extension/extension_helper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		Rf_warning("duckdb_finalize_database_R: Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
		           "duckdb::duckdb_shutdown(drv) to avoid this.");
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}
	return R_NilValue;
}

SEXP RApi::Startup(SEXP dbdirsexp, SEXP readonlysexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || Rf_length(dbdirsexp) != 1) {
		Rf_error("duckdb_startup_R: Need string parameter for dbdir");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));

	if (TYPEOF(readonlysexp) != LGLSXP || Rf_length(readonlysexp) != 1) {
		Rf_error("duckdb_startup_R: Need string parameter for read_only");
	}
	bool read_only = (bool)LOGICAL_ELT(readonlysexp, 0);

	if (strlen(dbdir) == 0 || strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	DBConfig config;
	config.access_mode = AccessMode::READ_WRITE;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}
	DuckDB *dbaddr;
	try {
		dbaddr = new DuckDB(dbdir, &config);
	} catch (std::exception &e) {
		Rf_error("duckdb_startup_R: Failed to open database: %s", e.what());
	}
	ExtensionHelper::LoadAllExtensions(*dbaddr);

	DataFrameScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	Connection conn(*dbaddr);
	auto &context = *conn.context;
	auto &catalog = Catalog::GetCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &info);
	context.transaction.Commit();

	RProtector r;

	SEXP dbsexp = r.Protect(R_MakeExternalPtr(dbaddr, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(dbsexp, (void (*)(SEXP))duckdb_finalize_database_R);
	return dbsexp;
}

SEXP RApi::Shutdown(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		Rf_error("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	DuckDB *dbaddr = (DuckDB *)R_ExternalPtrAddr(dbsexp);
	if (dbaddr) {
		R_ClearExternalPtr(dbsexp);
		delete dbaddr;
	}

	return R_NilValue;
}
