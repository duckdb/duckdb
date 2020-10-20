#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

using namespace std;

namespace duckdb {

static unique_ptr<FunctionData> read_csv_bind(ClientContext &context, vector<Value> &inputs,
                                              unordered_map<string, Value> &named_parameters,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_unique<ReadCSVData>();
	auto &options = result->options;

	string file_pattern = inputs[0].str_value;

	auto &fs = FileSystem::GetFileSystem(context);
	result->files = fs.Glob(file_pattern);
	if (result->files.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_pattern);
	}

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
			options.has_format[LogicalTypeId::DATE] = true;
			auto &date_format = options.date_format[LogicalTypeId::DATE];
			date_format.format_specifier = kv.second.str_value;
			string error = StrTimeFormat::ParseFormatSpecifier(date_format.format_specifier, date_format);
			if (!error.empty()) {
				throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
			}
		} else if (kv.first == "timestampformat") {
			options.has_format[LogicalTypeId::TIMESTAMP] = true;
			auto &timestamp_format = options.date_format[LogicalTypeId::TIMESTAMP];
			timestamp_format.format_specifier = kv.second.str_value;
			string error = StrTimeFormat::ParseFormatSpecifier(timestamp_format.format_specifier, timestamp_format);
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
		} else if (kv.first == "filename") {
			result->include_file_name = kv.second.value_.boolean;
		}
	}
	if (!options.auto_detect && return_types.size() == 0) {
		throw BinderException("Specifying CSV options requires columns to be specified as well (for now)");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		auto initial_reader = make_unique<BufferedCSVReader>(context, options);

		return_types.assign(initial_reader->sql_types.begin(), initial_reader->sql_types.end());
		names.assign(initial_reader->col_names.begin(), initial_reader->col_names.end());
		result->initial_reader = move(initial_reader);
	} else {
		result->sql_types = return_types;
		assert(return_types.size() == names.size());
	}
	if (result->include_file_name) {
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("filename");
	}
	return move(result);
}

struct ReadCSVOperatorData : public FunctionOperatorData {
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index;
};

static unique_ptr<FunctionOperatorData> read_csv_init(ClientContext &context, const FunctionData *bind_data_,
                                                      vector<column_t> &column_ids, TableFilterSet *table_filters) {
	auto &bind_data = (ReadCSVData &)*bind_data_;
	auto result = make_unique<ReadCSVOperatorData>();
	if (bind_data.initial_reader) {
		result->csv_reader = move(bind_data.initial_reader);
	} else {
		bind_data.options.file_path = bind_data.files[0];
		result->csv_reader = make_unique<BufferedCSVReader>(context, bind_data.options, bind_data.sql_types);
	}
	result->file_index = 1;
	return move(result);
}

static unique_ptr<FunctionData> read_csv_auto_bind(ClientContext &context, vector<Value> &inputs,
                                                   unordered_map<string, Value> &named_parameters,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	named_parameters["auto_detect"] = Value::BOOLEAN(true);
	return read_csv_bind(context, inputs, named_parameters, return_types, names);
}

static void read_csv_function(ClientContext &context, const FunctionData *bind_data_,
                              FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*bind_data_;
	auto &data = (ReadCSVOperatorData &)*operator_state;
	do {
		data.csv_reader->ParseCSV(output);
		if (output.size() == 0 && data.file_index < bind_data.files.size()) {
			// exhausted this file, but we have more files we can read
			// open the next file and increment the counter
			bind_data.options.file_path = bind_data.files[data.file_index];
			data.csv_reader = make_unique<BufferedCSVReader>(context, bind_data.options, data.csv_reader->sql_types);
			data.file_index++;
		} else {
			break;
		}
	} while (true);
	if (bind_data.include_file_name) {
		auto &col = output.data.back();
		col.SetValue(0, Value(data.csv_reader->options.file_path));
		col.vector_type = VectorType::CONSTANT_VECTOR;
	}
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
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
}

TableFunction ReadCSVTableFunction::GetFunction() {
	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, read_csv_function, read_csv_bind, read_csv_init);
	add_named_parameters(read_csv);
	return read_csv;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ReadCSVTableFunction::GetFunction());

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
