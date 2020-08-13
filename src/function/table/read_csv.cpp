#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

using namespace std;

namespace duckdb {

struct ReadCSVFunctionData : public TableFunctionData {
	ReadCSVFunctionData() {
	}

	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
};

static unique_ptr<FunctionData> read_csv_bind(ClientContext &context, vector<Value> &inputs, unordered_map<string, Value> &named_parameters,
                                              vector<LogicalType> &return_types, vector<string> &names) {

	if (!context.db.config.enable_copy) {
		throw Exception("read_csv is disabled by configuration");
	}
	auto result = make_unique<ReadCSVFunctionData>();

	BufferedCSVReaderOptions options;
	options.file_path = inputs[0].str_value;
	options.auto_detect = true;
	options.header = false;
	options.delimiter = ",";
	options.quote = "\"";

	for(auto &kv : named_parameters) {
		if (kv.first == "sep") {
			options.auto_detect = false;
			options.delimiter = kv.second.str_value;
		} else if (kv.first == "header") {
			options.auto_detect = false;
			options.header = kv.second.value_.boolean;
		} else if (kv.first == "quote") {
			options.auto_detect = false;
			options.quote = kv.second.str_value;
		} else if (kv.first == "escape") {
			options.auto_detect = false;
			options.escape = kv.second.str_value;
		} else if (kv.first == "nullstr") {
			options.auto_detect = false;
			options.null_str = kv.second.str_value;
		} else if (kv.first == "dateformat") {
			options.has_date_format = true;
			options.date_format.format_specifier = kv.second.str_value;
			string error = StrTimeFormat::ParseFormatSpecifier(kv.second.str_value, options.date_format);
			if (!error.empty()) {
				throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
			}
		} else if (kv.first == "timestampformat") {
			options.has_timestamp_format = true;
			options.timestamp_format.format_specifier = kv.second.str_value;
			string error = StrTimeFormat::ParseFormatSpecifier(kv.second.str_value, options.timestamp_format);
			if (!error.empty()) {
				throw InvalidInputException("Could not parse TIMESTAMPFORMAT: %s", error.c_str());
			}
		} else if (kv.first == "columns") {
			options.auto_detect = false;
			for (auto &val : kv.second.struct_value) {
				names.push_back(val.first);
				if (val.second.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				return_types.push_back(TransformStringToLogicalType(val.second.str_value.c_str()));
			}
			if (names.size() == 0) {
				throw BinderException("read_csv requires at least a single column as input!");
			}
		}
	}
	if (!options.auto_detect && return_types.size() == 0) {
		throw BinderException("Specifying CSV options requires columns to be specified as well (for now)");
	}
	if (return_types.size() > 0) {
		// return types specified: no auto detect
		result->csv_reader = make_unique<BufferedCSVReader>(context, move(options), return_types);
	} else {
		// auto detect options
		result->csv_reader = make_unique<BufferedCSVReader>(context, move(options));

		return_types.assign(result->csv_reader->sql_types.begin(), result->csv_reader->sql_types.end());
		names.assign(result->csv_reader->col_names.begin(), result->csv_reader->col_names.end());
	}
	return move(result);
}

static unique_ptr<FunctionData> read_csv_auto_bind(ClientContext &context, vector<Value> &inputs, unordered_map<string, Value> &named_parameters,
                                                   vector<LogicalType> &return_types, vector<string> &names) {

	if (!context.db.config.enable_copy) {
		throw Exception("read_csv_auto is disabled by configuration");
	}
	auto result = make_unique<ReadCSVFunctionData>();
	BufferedCSVReaderOptions options;
	options.auto_detect = true;
	options.file_path = inputs[0].str_value;

	result->csv_reader = make_unique<BufferedCSVReader>(context, move(options));

	// TODO: print detected dialect from result->csv_reader->info
	return_types.assign(result->csv_reader->sql_types.begin(), result->csv_reader->sql_types.end());
	names.assign(result->csv_reader->col_names.begin(), result->csv_reader->col_names.end());

	return move(result);
}

static void read_csv_info(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = ((ReadCSVFunctionData &)*dataptr);
	data.csv_reader->ParseCSV(output);
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet read_csv("read_csv");

	TableFunction read_csv_function = TableFunction({LogicalType::VARCHAR}, read_csv_bind, read_csv_info, nullptr);
	read_csv_function.named_parameters["sep"] = LogicalType::VARCHAR;
	read_csv_function.named_parameters["quote"] = LogicalType::VARCHAR;
	read_csv_function.named_parameters["escape"] = LogicalType::VARCHAR;
	read_csv_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	read_csv_function.named_parameters["columns"] = LogicalType::STRUCT;
	read_csv_function.named_parameters["header"] = LogicalType::BOOLEAN;
	read_csv_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	read_csv_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;

	read_csv.AddFunction(move(read_csv_function));

	set.AddFunction(read_csv);
	set.AddFunction(TableFunction("read_csv_auto", {LogicalType::VARCHAR}, read_csv_auto_bind, read_csv_info, nullptr));
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
