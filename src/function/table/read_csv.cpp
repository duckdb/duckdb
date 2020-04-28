#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

using namespace std;

namespace duckdb {

struct ReadCSVData : public TableFunctionData {
	ReadCSVData() {
	}

	CopyInfo info;
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

static unique_ptr<FunctionData> read_csv_bind(ClientContext &context, vector<Value> inputs,
                                              vector<SQLType> &return_types, vector<string> &names) {
	for (auto &val : inputs[2].struct_value) {
		names.push_back(val.first);
		if (val.second.type != TypeId::VARCHAR) {
			throw BinderException("read_csv requires a type specification as string");
		}
		return_types.push_back(TransformStringToSQLType(val.second.str_value.c_str()));
	}
	if (names.size() == 0) {
		throw BinderException("read_csv requires at least a single column as input!");
	}
	auto result = make_unique<ReadCSVData>();

	result->info.file_path = inputs[0].str_value;
	result->info.header = false;
	result->info.delimiter = inputs[1].str_value;

	result->csv_reader = make_unique<BufferedCSVReader>(context, result->info, return_types);
	return move(result);
}

static unique_ptr<FunctionData> read_csv_auto_bind(ClientContext &context, vector<Value> inputs,
                                                   vector<SQLType> &return_types, vector<string> &names) {
	auto result = make_unique<ReadCSVData>();
	result->info.file_path = inputs[0].str_value;
	result->info.auto_detect = true;

	result->csv_reader = make_unique<BufferedCSVReader>(context, result->info);

	// TODO: print detected dialect from result->csv_reader->info
	return_types.assign(result->csv_reader->sql_types.begin(), result->csv_reader->sql_types.end());
	names.assign(result->csv_reader->col_names.begin(), result->csv_reader->col_names.end());

	return move(result);
}

static void read_csv_info(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = ((ReadCSVData &)*dataptr);
	data.csv_reader->ParseCSV(output);
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("read_csv", {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::STRUCT}, read_csv_bind,
	                              read_csv_info, nullptr));
	set.AddFunction(TableFunction("read_csv_auto", {SQLType::VARCHAR}, read_csv_auto_bind, read_csv_info, nullptr));
}

void BuiltinFunctions::RegisterReadFunctions() {
	ReadCSVTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
