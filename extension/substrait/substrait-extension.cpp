
#include "substrait-extension.hpp"
//
//#ifndef DUCKDB_AMALGAMATION
//#include "duckdb/function/table_function.hpp"
//#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
//#include "duckdb/parser/parsed_data/create_view_info.hpp"
//#include "duckdb/parser/parser.hpp"
//#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
//#endif

namespace duckdb {

// struct DBGenFunctionData : public TableFunctionData {
//	DBGenFunctionData() {
//	}
//
//	bool finished = false;
//	double sf = 0;
//	string schema = DEFAULT_SCHEMA;
//	string suffix;
//	bool overwrite = false;
// };
//
// static unique_ptr<FunctionData> DbgenBind(ClientContext &context, vector<Value> &inputs,
//                                           named_parameter_map_t &named_parameters,
//                                           vector<LogicalType> &input_table_types, vector<string> &input_table_names,
//                                           vector<LogicalType> &return_types, vector<string> &names) {
//	auto result = make_unique<DBGenFunctionData>();
//	for (auto &kv : named_parameters) {
//		if (kv.first == "sf") {
//			result->sf = DoubleValue::Get(kv.second);
//		} else if (kv.first == "schema") {
//			result->schema = StringValue::Get(kv.second);
//		} else if (kv.first == "suffix") {
//			result->suffix = StringValue::Get(kv.second);
//		} else if (kv.first == "overwrite") {
//			result->overwrite = BooleanValue::Get(kv.second);
//		}
//	}
//	return_types.emplace_back(LogicalType::BOOLEAN);
//	names.emplace_back("Success");
//	return move(result);
// }
//
// static void DbgenFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData
// *operator_state,
//                           DataChunk *input, DataChunk &output) {
//	auto &data = (DBGenFunctionData &)*bind_data;
//	if (data.finished) {
//		return;
//	}
//	tpch::DBGenWrapper::CreateTPCHSchema(context, data.schema, data.suffix);
//	tpch::DBGenWrapper::LoadTPCHData(context, data.sf, data.schema, data.suffix);
//
//	data.finished = true;
// }
//
// struct TPCHData : public FunctionOperatorData {
//	TPCHData() : offset(0) {
//	}
//	idx_t offset;
// };
//
// unique_ptr<FunctionOperatorData> TPCHInit(ClientContext &context, const FunctionData *bind_data,
//                                           const vector<column_t> &column_ids, TableFilterCollection *filters) {
//	auto result = make_unique<TPCHData>();
//	return move(result);
// }
//
// static unique_ptr<FunctionData> TPCHQueryBind(ClientContext &context, vector<Value> &inputs,
//                                               named_parameter_map_t &named_parameters,
//                                               vector<LogicalType> &input_table_types, vector<string>
//                                               &input_table_names, vector<LogicalType> &return_types, vector<string>
//                                               &names) {
//	names.emplace_back("query_nr");
//	return_types.emplace_back(LogicalType::INTEGER);
//
//	names.emplace_back("query");
//	return_types.emplace_back(LogicalType::VARCHAR);
//
//	return nullptr;
// }
//
// static void TPCHQueryFunction(ClientContext &context, const FunctionData *bind_data,
//                               FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
//	auto &data = (TPCHData &)*operator_state;
//	idx_t tpch_queries = 22;
//	if (data.offset >= tpch_queries) {
//		// finished returning values
//		return;
//	}
//	idx_t chunk_count = 0;
//	while (data.offset < tpch_queries && chunk_count < STANDARD_VECTOR_SIZE) {
//		auto query = tpch::DBGenWrapper::GetQuery(data.offset + 1);
//		// "query_nr", PhysicalType::INT32
//		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)data.offset + 1));
//		// "query", PhysicalType::VARCHAR
//		output.SetValue(1, chunk_count, Value(query));
//		data.offset++;
//		chunk_count++;
//	}
//	output.SetCardinality(chunk_count);
// }
//
// static unique_ptr<FunctionData> TPCHQueryAnswerBind(ClientContext &context, vector<Value> &inputs,
//                                                     named_parameter_map_t &named_parameters,
//                                                     vector<LogicalType> &input_table_types,
//                                                     vector<string> &input_table_names,
//                                                     vector<LogicalType> &return_types, vector<string> &names) {
//	names.emplace_back("query_nr");
//	return_types.emplace_back(LogicalType::INTEGER);
//
//	names.emplace_back("scale_factor");
//	return_types.emplace_back(LogicalType::DOUBLE);
//
//	names.emplace_back("answer");
//	return_types.emplace_back(LogicalType::VARCHAR);
//
//	return nullptr;
// }
//
// static void TPCHQueryAnswerFunction(ClientContext &context, const FunctionData *bind_data,
//                                     FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
//	auto &data = (TPCHData &)*operator_state;
//	idx_t tpch_queries = 22;
//	vector<double> scale_factors {0.01, 0.1, 1};
//	idx_t total_answers = tpch_queries * scale_factors.size();
//	if (data.offset >= total_answers) {
//		// finished returning values
//		return;
//	}
//	idx_t chunk_count = 0;
//	while (data.offset < total_answers && chunk_count < STANDARD_VECTOR_SIZE) {
//		idx_t cur_query = data.offset % tpch_queries;
//		idx_t cur_sf = data.offset / tpch_queries;
//		auto answer = tpch::DBGenWrapper::GetAnswer(scale_factors[cur_sf], cur_query + 1);
//		// "query_nr", PhysicalType::INT32
//		output.SetValue(0, chunk_count, Value::INTEGER((int32_t)cur_query + 1));
//		// "scale_factor", PhysicalType::INT32
//		output.SetValue(1, chunk_count, Value::DOUBLE(scale_factors[cur_sf]));
//		// "query", PhysicalType::VARCHAR
//		output.SetValue(2, chunk_count, Value(answer));
//		data.offset++;
//		chunk_count++;
//	}
//	output.SetCardinality(chunk_count);
// }
//
static string PragmaGetSubstrait(ClientContext &context, const FunctionParameters &parameters) {
	auto query = parameters.values[0].GetValue<string>();
	return query;
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &catalog = Catalog::GetCatalog(*con.context);

	// create the get_substrait pragma that allows us to get a substrait binary from a valid SQL Query
	auto get_substrait_func = PragmaFunction::PragmaCall("get_substrait", PragmaGetSubstrait, {LogicalType::BLOB});
	CreatePragmaFunctionInfo info(get_substrait_func);
	catalog.CreatePragmaFunction(*con.context, &info);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}