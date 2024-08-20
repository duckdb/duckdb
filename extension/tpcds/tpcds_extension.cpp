#define DUCKDB_EXTENSION_MAIN
#include "tpcds_extension.hpp"

#include "dsdgen.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#endif

namespace duckdb {

struct DSDGenFunctionData : public TableFunctionData {
	DSDGenFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string catalog = INVALID_CATALOG;
	string schema = DEFAULT_SCHEMA;
	string suffix;
	bool overwrite = false;
	bool keys = false;
};

static duckdb::unique_ptr<FunctionData> DsdgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DSDGenFunctionData>();
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("Cannot use NULL as function argument");
		}
		if (kv.first == "sf") {
			result->sf = kv.second.GetValue<double>();
		} else if (kv.first == "catalog") {
			result->catalog = StringValue::Get(kv.second);
		} else if (kv.first == "schema") {
			result->schema = StringValue::Get(kv.second);
		} else if (kv.first == "suffix") {
			result->suffix = StringValue::Get(kv.second);
		} else if (kv.first == "overwrite") {
			result->overwrite = kv.second.GetValue<bool>();
		} else if (kv.first == "keys") {
			result->keys = kv.second.GetValue<bool>();
		}
	}
	if (input.binder) {
		auto &catalog = Catalog::GetCatalog(context, result->catalog);
		auto &properties = input.binder->GetStatementProperties();
		properties.RegisterDBModify(catalog, context);
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void DsdgenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DSDGenFunctionData>();
	if (data.finished) {
		return;
	}
	tpcds::DSDGenWrapper::CreateTPCDSSchema(context, data.catalog, data.schema, data.suffix, data.keys, data.overwrite);
	tpcds::DSDGenWrapper::DSDGen(data.sf, context, data.catalog, data.schema, data.suffix);

	data.finished = true;
}

struct TPCDSData : public GlobalTableFunctionState {
	TPCDSData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> TPCDSInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<TPCDSData>();
	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> TPCDSQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void TPCDSQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TPCDSData>();
	idx_t tpcds_queries = tpcds::DSDGenWrapper::QueriesCount();
	if (data.offset >= tpcds_queries) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < tpcds_queries && chunk_count < STANDARD_VECTOR_SIZE) {
		auto query = TpcdsExtension::GetQuery(data.offset + 1);
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
		// "query", PhysicalType::VARCHAR
		output.SetValue(1, chunk_count, Value(query));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static duckdb::unique_ptr<FunctionData> TPCDSQueryAnswerBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("answer");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void TPCDSQueryAnswerFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TPCDSData>();
	idx_t tpcds_queries = tpcds::DSDGenWrapper::QueriesCount();
	vector<double> scale_factors {1, 10};
	idx_t total_answers = tpcds_queries * scale_factors.size();
	if (data.offset >= total_answers) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < total_answers && chunk_count < STANDARD_VECTOR_SIZE) {
		idx_t cur_query = data.offset % tpcds_queries;
		idx_t cur_sf = data.offset / tpcds_queries;
		auto answer = TpcdsExtension::GetAnswer(scale_factors[cur_sf], cur_query + 1);
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)cur_query + 1));
		// "scale_factor", PhysicalType::DOUBLE
		output.SetValue(1, chunk_count, Value::DOUBLE(scale_factors[cur_sf]));
		// "query", PhysicalType::VARCHAR
		output.SetValue(2, chunk_count, Value(answer));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static string PragmaTpcdsQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto index = parameters.values[0].GetValue<int32_t>();
	return tpcds::DSDGenWrapper::GetQuery(index);
}

static void LoadInternal(DuckDB &db) {
	auto &db_instance = *db.instance;

	TableFunction dsdgen_func("dsdgen", {}, DsdgenFunction, DsdgenBind);
	dsdgen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	dsdgen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	dsdgen_func.named_parameters["keys"] = LogicalType::BOOLEAN;
	dsdgen_func.named_parameters["catalog"] = LogicalType::VARCHAR;
	dsdgen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	dsdgen_func.named_parameters["suffix"] = LogicalType::VARCHAR;
	ExtensionUtil::RegisterFunction(db_instance, dsdgen_func);

	// create the TPCDS pragma that allows us to run the query
	auto tpcds_func = PragmaFunction::PragmaCall("tpcds", PragmaTpcdsQuery, {LogicalType::BIGINT});
	ExtensionUtil::RegisterFunction(db_instance, tpcds_func);

	// create the TPCDS_QUERIES function that returns the query
	TableFunction tpcds_query_func("tpcds_queries", {}, TPCDSQueryFunction, TPCDSQueryBind, TPCDSInit);
	ExtensionUtil::RegisterFunction(db_instance, tpcds_query_func);

	// create the TPCDS_ANSWERS that returns the query result
	TableFunction tpcds_query_answer_func("tpcds_answers", {}, TPCDSQueryAnswerFunction, TPCDSQueryAnswerBind,
	                                      TPCDSInit);
	ExtensionUtil::RegisterFunction(db_instance, tpcds_query_answer_func);
}

void TpcdsExtension::Load(DuckDB &db) {
	LoadInternal(db);
}

std::string TpcdsExtension::GetQuery(int query) {
	return tpcds::DSDGenWrapper::GetQuery(query);
}

std::string TpcdsExtension::GetAnswer(double sf, int query) {
	return tpcds::DSDGenWrapper::GetAnswer(sf, query);
}

std::string TpcdsExtension::Name() {
	return "tpcds";
}

std::string TpcdsExtension::Version() const {
#ifdef EXT_VERSION_TPCDS
	return EXT_VERSION_TPCDS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void tpcds_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *tpcds_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
