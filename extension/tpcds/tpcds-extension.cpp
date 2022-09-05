#define DUCKDB_EXTENSION_MAIN

#include "tpcds-extension.hpp"

#include "dsdgen.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#endif

namespace duckdb {

struct DSDGenFunctionData : public TableFunctionData {
	DSDGenFunctionData() {
	}

	bool finished = false;
	double sf = 0;
	string schema = DEFAULT_SCHEMA;
	string suffix;
	bool overwrite = false;
	bool keys = false;
};

static unique_ptr<FunctionData> DsdgenBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<DSDGenFunctionData>();
	for (auto &kv : input.named_parameters) {
		if (kv.first == "sf") {
			result->sf = kv.second.GetValue<double>();
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
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return move(result);
}

static void DsdgenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DSDGenFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	tpcds::DSDGenWrapper::CreateTPCDSSchema(context, data.schema, data.suffix, data.keys, data.overwrite);
	tpcds::DSDGenWrapper::DSDGen(data.sf, context, data.schema, data.suffix);

	data.finished = true;
}

struct TPCDSData : public GlobalTableFunctionState {
	TPCDSData() : offset(0) {
	}
	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> TPCDSInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<TPCDSData>();
	return move(result);
}

static unique_ptr<FunctionData> TPCDSQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_nr");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static void TPCDSQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (TPCDSData &)*data_p.global_state;
	idx_t tpcds_queries = tpcds::DSDGenWrapper::QueriesCount();
	if (data.offset >= tpcds_queries) {
		// finished returning values
		return;
	}
	idx_t chunk_count = 0;
	while (data.offset < tpcds_queries && chunk_count < STANDARD_VECTOR_SIZE) {
		auto query = TPCDSExtension::GetQuery(data.offset + 1);
		// "query_nr", PhysicalType::INT32
		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
		// "query", PhysicalType::VARCHAR
		output.SetValue(1, chunk_count, Value(query));
		data.offset++;
		chunk_count++;
	}
	output.SetCardinality(chunk_count);
}

static unique_ptr<FunctionData> TPCDSQueryAnswerBind(ClientContext &context, TableFunctionBindInput &input,
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
	auto &data = (TPCDSData &)*data_p.global_state;
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
		auto answer = TPCDSExtension::GetAnswer(scale_factors[cur_sf], cur_query + 1);
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

void TPCDSExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	TableFunction dsdgen_func("dsdgen", {}, DsdgenFunction, DsdgenBind);
	dsdgen_func.named_parameters["sf"] = LogicalType::DOUBLE;
	dsdgen_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	dsdgen_func.named_parameters["keys"] = LogicalType::BOOLEAN;
	dsdgen_func.named_parameters["schema"] = LogicalType::VARCHAR;
	dsdgen_func.named_parameters["suffix"] = LogicalType::VARCHAR;
	CreateTableFunctionInfo dsdgen_info(dsdgen_func);

	// create the dsdgen function
	auto &catalog = Catalog::GetCatalog(*con.context);
	catalog.CreateTableFunction(*con.context, &dsdgen_info);

	// create the TPCDS pragma that allows us to run the query
	auto tpcds_func = PragmaFunction::PragmaCall("tpcds", PragmaTpcdsQuery, {LogicalType::BIGINT});
	CreatePragmaFunctionInfo info(tpcds_func);
	catalog.CreatePragmaFunction(*con.context, &info);

	// create the TPCDS_QUERIES function that returns the query
	TableFunction tpcds_query_func("tpcds_queries", {}, TPCDSQueryFunction, TPCDSQueryBind, TPCDSInit);
	CreateTableFunctionInfo tpcds_query_info(tpcds_query_func);
	catalog.CreateTableFunction(*con.context, &tpcds_query_info);

	// create the TPCDS_ANSWERS that returns the query result
	TableFunction tpcds_query_answer_func("tpcds_answers", {}, TPCDSQueryAnswerFunction, TPCDSQueryAnswerBind,
	                                      TPCDSInit);
	CreateTableFunctionInfo tpcds_query_asnwer_info(tpcds_query_answer_func);
	catalog.CreateTableFunction(*con.context, &tpcds_query_asnwer_info);

	con.Commit();
}

std::string TPCDSExtension::GetQuery(int query) {
	return tpcds::DSDGenWrapper::GetQuery(query);
}

std::string TPCDSExtension::GetAnswer(double sf, int query) {
	return tpcds::DSDGenWrapper::GetAnswer(sf, query);
}

std::string TPCDSExtension::Name() {
	return "tpcds";
}

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void tpcds_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::TPCDSExtension>();
}

DUCKDB_EXTENSION_API const char *tpcds_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
