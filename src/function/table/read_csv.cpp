#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

using namespace std;

namespace duckdb {

struct ReadCSVFunctionData : public TableFunctionData {
	ReadCSVFunctionData() : is_consumed(false) {
	}

	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
	//! Whether or not the CSV has already been read completely
	bool is_consumed;
};

static unique_ptr<FunctionData> read_csv_bind(ClientContext &context, vector<Value> &inputs,
                                              unordered_map<string, Value> &named_parameters,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ReadCSVFunctionData>();

	BufferedCSVReaderOptions options;
	options.file_path = inputs[0].str_value;
	options.auto_detect = false;
	options.header = false;
	options.delimiter = ",";
	options.quote = "\"";

	for (auto &kv : named_parameters) {
		if (kv.first == "auto_detect") {
			options.auto_detect = kv.second.value_.boolean;
		} else if (kv.first == "sep" || kv.first == "delim") {
			options.delimiter = kv.second.str_value;
			options.has_delimiter = true;
		} else if (kv.first == "header") {
			options.header = kv.second.value_.boolean;
			options.has_header = true;
		} else if (kv.first == "quote") {
			options.quote = kv.second.str_value;
			options.has_quote = true;
		} else if (kv.first == "escape") {
			options.escape = kv.second.str_value;
			options.has_escape = true;
		} else if (kv.first == "nullstr") {
			options.null_str = kv.second.str_value;
		} else if (kv.first == "sample_size") {
			options.sample_size = kv.second.GetValue<int64_t>();
			if (options.sample_size > STANDARD_VECTOR_SIZE) {
				throw BinderException(
				    "Unsupported parameter for SAMPLE_SIZE: cannot be bigger than STANDARD_VECTOR_SIZE %d",
				    STANDARD_VECTOR_SIZE);
			} else if (options.sample_size < 1) {
				throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
			}
		} else if (kv.first == "num_samples") {
			options.num_samples = kv.second.GetValue<int64_t>();
			if (options.num_samples < 1) {
				throw BinderException("Unsupported parameter for NUM_SAMPLES: cannot be smaller than 1");
			}
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

static unique_ptr<FunctionOperatorData> read_csv_init(ClientContext &context, const FunctionData *bind_data_,
                                                      OperatorTaskInfo *task_info, vector<column_t> &column_ids,
                                                      unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto &bind_data = (ReadCSVFunctionData &)*bind_data_;
	if (bind_data.is_consumed) {
		bind_data.csv_reader =
		    make_unique<BufferedCSVReader>(context, bind_data.csv_reader->options, bind_data.csv_reader->sql_types);
	}
	bind_data.is_consumed = true;
	return nullptr;
}

static unique_ptr<FunctionData> read_csv_auto_bind(ClientContext &context, vector<Value> &inputs,
                                                   unordered_map<string, Value> &named_parameters,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	named_parameters["auto_detect"] = Value::BOOLEAN(true);
	return read_csv_bind(context, inputs, named_parameters, return_types, names);
}

static void read_csv_function(ClientContext &context, const FunctionData *bind_data,
                              FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (ReadCSVFunctionData &)*bind_data;
	data.csv_reader->ParseCSV(output);
}

static void add_named_parameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	table_function.named_parameters["columns"] = LogicalType::STRUCT;
	table_function.named_parameters["header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["num_samples"] = LogicalType::BIGINT;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {

	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, read_csv_function, read_csv_bind, read_csv_init);
	add_named_parameters(read_csv);
	set.AddFunction(read_csv);

	TableFunction read_csv_auto("read_csv_auto", {LogicalType::VARCHAR}, read_csv_function, read_csv_auto_bind,
	                            read_csv_init);
	add_named_parameters(read_csv_auto);
	set.AddFunction(read_csv_auto);
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
}

} // namespace duckdb
