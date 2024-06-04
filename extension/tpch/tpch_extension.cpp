#define DUCKDB_EXTENSION_MAIN

#include "tpch_extension.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/transaction/transaction.hpp"
#endif

#include "dbgen/dbgen.hpp"

namespace duckdb {

struct DBGenFunctionData : public TableFunctionData {
	DBGenFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string catalog = INVALID_CATALOG;
	string schema = DEFAULT_SCHEMA;
	string suffix;
	bool overwrite = false;
	uint32_t children = 1;
	int step = -1;
};

static duckdb::unique_ptr<FunctionData> DbgenBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DBGenFunctionData>();
	for (auto &kv : input.named_parameters) {
		if (kv.first == "sf") {
			result->sf = DoubleValue::Get(kv.second);
		} else if (kv.first == "catalog") {
			result->catalog = StringValue::Get(kv.second);
		} else if (kv.first == "schema") {
			result->schema = StringValue::Get(kv.second);
		} else if (kv.first == "suffix") {
			result->suffix = StringValue::Get(kv.second);
		} else if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		} else if (kv.first == "children") {
			result->children = UIntegerValue::Get(kv.second);
		} else if (kv.first == "step") {
			result->step = UIntegerValue::Get(kv.second);
		}
	}
	if (result->children != 1 && result->step == -1) {
		throw InvalidInputException("Step must be defined when children are defined");
	}
	if (input.binder) {
		auto &catalog = Catalog::GetCatalog(context, result->catalog);
		auto &properties = input.binder->GetStatementProperties();
		properties.modified_databases.insert(catalog.GetName());
	}
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void DbgenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<DBGenFunctionData>();
	if (data.finished) {
		return;
	}
	tpch::DBGenWrapper::CreateTPCHSchema(context, data.catalog, data.schema, data.suffix);
	tpch::DBGenWrapper::LoadTPCHData(context, data.sf, data.catalog, data.schema, data.suffix, data.children,
	                                 data.step);

	data.finished = true;
}

struct TPCHData : public GlobalTableFunctionState {
	TPCHData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> TPCHInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<TPCHData>();
	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> TPCHQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void TPCHQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TPCHData>();
	idx_t tpch_queries = 22;
	if (data.offset >= tpch_queries) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < tpch_queries && chunk_count < STANDARD_VECTOR_SIZE) {
		auto query = tpch::DBGenWrapper::GetQuery(data.offset + 1);
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
		// "query", PhysicalType::VARCHAR
		output.SetValue(1, chunk_count, Value(query));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static duckdb::unique_ptr<FunctionData> TPCHQueryAnswerBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("scale_factor");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("answer");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void TPCHQueryAnswerFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<TPCHData>();
	idx_t tpch_queries = 22;
	vector<double> scale_factors {0.01, 0.1, 1};
	idx_t total_answers = tpch_queries * scale_factors.size();
	if (data.offset >= total_answers) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < total_answers && chunk_count < STANDARD_VECTOR_SIZE) {
		idx_t cur_query = data.offset % tpch_queries;
		idx_t cur_sf = data.offset / tpch_queries;
		auto answer = tpch::DBGenWrapper::GetAnswer(scale_factors[cur_sf], cur_query + 1);
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)cur_query + 1));
		// "scale_factor", PhysicalType::INT32
		output.SetValue(1, chunk_count, Value::DOUBLE(scale_factors[cur_sf]));
		// "query", PhysicalType::VARCHAR
		output.SetValue(2, chunk_count, Value(answer));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static string PragmaTpchQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto index = parameters.values[0].GetValue<int32_t>();
	return tpch::DBGenWrapper::GetQuery(index);
}

static void LoadInternal(DuckDB &db) {
	auto &db_instance = *db.instance;

	TableFunction dbgen_func("dbgen", {}, DbgenFunction, DbgenBind);
	dbgen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	dbgen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	dbgen_func.named_parameters["catalog"] = LogicalType::VARCHAR;
	dbgen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	dbgen_func.named_parameters["suffix"] = LogicalType::VARCHAR;
	dbgen_func.named_parameters["children"] = LogicalType::UINTEGER;
	dbgen_func.named_parameters["step"] = LogicalType::UINTEGER;
	ExtensionUtil::RegisterFunction(db_instance, dbgen_func);

	// create the TPCH pragma that allows us to run the query
	auto tpch_func = PragmaFunction::PragmaCall("tpch", PragmaTpchQuery, {LogicalType::BIGINT});
	ExtensionUtil::RegisterFunction(db_instance, tpch_func);

	// create the TPCH_QUERIES function that returns the query
	TableFunction tpch_query_func("tpch_queries", {}, TPCHQueryFunction, TPCHQueryBind, TPCHInit);
	ExtensionUtil::RegisterFunction(db_instance, tpch_query_func);

	// create the TPCH_ANSWERS that returns the query result
	TableFunction tpch_query_answer_func("tpch_answers", {}, TPCHQueryAnswerFunction, TPCHQueryAnswerBind, TPCHInit);
	ExtensionUtil::RegisterFunction(db_instance, tpch_query_answer_func);
}

void TpchExtension::Load(DuckDB &db) {
	LoadInternal(db);
}

std::string TpchExtension::GetQuery(int query) {
	return tpch::DBGenWrapper::GetQuery(query);
}

std::string TpchExtension::GetAnswer(double sf, int query) {
	return tpch::DBGenWrapper::GetAnswer(sf, query);
}

std::string TpchExtension::Name() {
	return "tpch";
}

std::string TpchExtension::Version() const {
#ifdef EXT_VERSION_TPCH
	return EXT_VERSION_TPCH;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void tpch_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *tpch_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
