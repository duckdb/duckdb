#include "rapi.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;

void duckdb::DBDeleter(DBWrapper *db) {
	cpp11::warning("Database is garbage-collected, use dbDisconnect(con, shutdown=TRUE) or "
	               "duckdb::duckdb_shutdown(drv) to avoid this.");
	delete db;
}

static bool CastRstringToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<PrimitiveType<uintptr_t>, PrimitiveType<string_t>>(
	    source, result, count,
	    [&](PrimitiveType<uintptr_t> input) { return StringVector::AddString(result, (const char *)input.val); });
	return true;
}

struct ZeroSumOperation {
	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetValid(idx);
			target[idx] = 0;
		} else {
			target[idx] = state->value;
		}
	}
};

[[cpp11::register]] void rapi_set_sum_default_to_zero(duckdb::conn_eptr_t conn) {
	// I want to check the validity of conn, but if I do, an R program that calls this function
	// will not exit

	conn->conn->context->transaction.BeginTransaction();
	auto sum_function = Catalog::GetEntry(*conn->conn->context, CatalogType::AGGREGATE_FUNCTION_ENTRY, SYSTEM_CATALOG,
	                                      DEFAULT_SCHEMA, "sum", false);

	auto sum_function_cast = (AggregateFunctionCatalogEntry *)sum_function;
	for (auto &aggr : sum_function_cast->functions.functions) {
		switch (aggr.arguments[0].InternalType()) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::DOUBLE:
		case PhysicalType::FLOAT:
			aggr.finalize = AggregateFunction::StateFinalize<SumState<int64_t>, int64_t, ZeroSumOperation>;
			break;
		case PhysicalType::BOOL:
		case PhysicalType::BIT:
		case PhysicalType::STRUCT:
		case PhysicalType::INTERVAL:
		case PhysicalType::LIST:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
		default:
			continue;
		}
	}

	conn->conn->context->transaction.Commit();
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
		try {
			config.SetOptionByName(key, Value(val));
		} catch (std::exception &e) {
			cpp11::stop("rapi_startup: Failed to set configuration option: %s", e.what());
		}
	}

	DBWrapper *wrapper;

	try {
		wrapper = new DBWrapper();

		auto data = make_unique<ArrowScanReplacementData>();
		data->wrapper = wrapper;
		config.replacement_scans.emplace_back(ArrowScanReplacement, std::move(data));
		wrapper->db = make_unique<DuckDB>(dbdirchar, &config);
	} catch (std::exception &e) {
		cpp11::stop("rapi_startup: Failed to open database: %s", e.what());
	}
	D_ASSERT(wrapper->db);

	DataFrameScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	Connection conn(*wrapper->db);
	auto &context = *conn.context;
	auto &catalog = Catalog::GetSystemCatalog(context);
	context.transaction.BeginTransaction();

	catalog.CreateTableFunction(context, &info);

	auto &runtime_config = DBConfig::GetConfig(context);

	auto &casts = runtime_config.GetCastFunctions();
	casts.RegisterCastFunction(RStringsType::Get(), LogicalType::VARCHAR, CastRstringToVarchar);

	context.transaction.Commit();

	return db_eptr_t(wrapper);
}

[[cpp11::register]] void rapi_shutdown(duckdb::db_eptr_t dbsexp) {
	auto db_wrapper = dbsexp.release();
	if (db_wrapper) {
		delete db_wrapper;
	}
}
