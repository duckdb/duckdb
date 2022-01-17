#include "cpp11/protect.hpp"

#include "rapi.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

static SEXP duckdb_finalize_database_R(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	auto db_wrapper = (DBWrapper *)R_ExternalPtrAddr(dbsexp);
	if (db_wrapper) {
		cpp11::warning("duckdb_finalize_database_R: Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
		           "duckdb::duckdb_shutdown(drv) to avoid this.");
		R_ClearExternalPtr(dbsexp);
		delete db_wrapper;
	}
	return R_NilValue;
}

SEXP RApi::Startup(SEXP dbdirsexp, SEXP readonlysexp, SEXP configsexp) {
	if (TYPEOF(dbdirsexp) != STRSXP || Rf_length(dbdirsexp) != 1) {
		cpp11::stop("duckdb_startup_R: Need string parameter for dbdir");
	}
	char *dbdir = (char *)CHAR(STRING_ELT(dbdirsexp, 0));

	if (TYPEOF(readonlysexp) != LGLSXP || Rf_length(readonlysexp) != 1) {
		cpp11::stop("duckdb_startup_R: Need string parameter for read_only");
	}
	bool read_only = (bool)LOGICAL_ELT(readonlysexp, 0);

	if (strlen(dbdir) == 0 || strcmp(dbdir, ":memory:") == 0) {
		dbdir = NULL;
	}

	DBConfig config;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}

	RProtector r;
	auto confignamessexp = r.Protect(GET_NAMES(configsexp));

	for (idx_t i = 0; i < (idx_t)Rf_length(configsexp); i++) {
		string key = string(CHAR(STRING_ELT(confignamessexp, i)));
		string val = string(CHAR(STRING_ELT(VECTOR_ELT(configsexp, i), 0)));
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			cpp11::stop("Unrecognized configuration property '%s'", key.c_str());
		}
		try {
			config.SetOption(*config_property, Value(val));
		} catch (std::exception &e) {
			cpp11::stop("duckdb_startup_R: Failed to set configuration option: %s", e.what());
		}
	}

	DBWrapper *wrapper;

	try {
		wrapper = new DBWrapper();
		config.replacement_scans.emplace_back(ArrowScanReplacement, wrapper);
		wrapper->db = make_unique<DuckDB>(dbdir, &config);
	} catch (std::exception &e) {
		cpp11::stop("duckdb_startup_R: Failed to open database: %s", e.what());
	}
	D_ASSERT(wrapper->db);

	DataFrameScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	Connection conn(*wrapper->db);
	auto &context = *conn.context;
	auto &catalog = Catalog::GetCatalog(context);
	context.transaction.BeginTransaction();
	catalog.CreateTableFunction(context, &info);
	context.transaction.Commit();

	SEXP dbsexp = r.Protect(R_MakeExternalPtr(wrapper, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(dbsexp, (void (*)(SEXP))duckdb_finalize_database_R);
	return dbsexp;
}

SEXP RApi::Shutdown(SEXP dbsexp) {
	if (TYPEOF(dbsexp) != EXTPTRSXP) {
		cpp11::stop("duckdb_finalize_connection_R: Need external pointer parameter");
	}
	auto db_wrapper = (DBWrapper *)R_ExternalPtrAddr(dbsexp);
	if (db_wrapper) {
		R_ClearExternalPtr(dbsexp);
		delete db_wrapper;
	}

	return R_NilValue;
}
