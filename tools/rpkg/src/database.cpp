#include "rapi.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

void duckdb::DBDeleter(DBWrapper *db) {
	cpp11::warning("Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
	               "duckdb::duckdb_shutdown(drv) to avoid this.");
	delete db;
}

[[cpp11::register]] duckdb::db_eptr_t rapi_startup(std::string dbdir, bool readonly, cpp11::list configsexp) {

	const char *dbdirchar;

	if (dbdir.length() == 0 || dbdir.compare(":memory:") == 0) {
		dbdirchar = NULL;
	} else {
		dbdirchar = dbdir.c_str();
	}

	DBConfig config;
	if (readonly) {
		config.options.access_mode = AccessMode::READ_ONLY;
	}

	auto confignames = configsexp.names();

	for (auto it = confignames.begin(); it != confignames.end(); ++it) {
		std::string key = *it;
		std::string val = cpp11::as_cpp<std::string>(configsexp[key]);
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			cpp11::stop("rapi_startup: Unrecognized configuration property '%s'", key.c_str());
		}
		try {
			config.SetOption(*config_property, Value(val));
		} catch (std::exception &e) {
			cpp11::stop("rapi_startup: Failed to set configuration option: %s", e.what());
		}
	}

	DBWrapper *wrapper;

	try {
		wrapper = new DBWrapper();

		auto data = make_unique<ArrowScanReplacementData>();
		data->wrapper = wrapper;
		config.replacement_scans.emplace_back(ArrowScanReplacement, move(data));
		wrapper->db = make_unique<DuckDB>(dbdirchar, &config);
	} catch (std::exception &e) {
		cpp11::stop("rapi_startup: Failed to open database: %s", e.what());
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

	return db_eptr_t(wrapper);
}

[[cpp11::register]] void rapi_shutdown(duckdb::db_eptr_t dbsexp) {
	auto db_wrapper = dbsexp.release();
	if (db_wrapper) {
		delete db_wrapper;
	}
}
