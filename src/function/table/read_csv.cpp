#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <limits>

namespace duckdb {

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning CSV files is disabled through configuration");
	}
	auto result = make_unique<ReadCSVData>();
	auto &options = result->options;

	auto &file_pattern = StringValue::Get(input.inputs[0]);

	auto &fs = FileSystem::GetFileSystem(context);
	result->files = fs.Glob(file_pattern, context);
	if (result->files.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_pattern);
	}

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_csv columns requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val)));
			}
			if (names.empty()) {
				throw BinderException("read_csv requires at least a single column as input!");
			}
		} else if (loption == "all_varchar") {
			options.all_varchar = BooleanValue::Get(kv.second);
		} else if (loption == "normalize_names") {
			options.normalize_names = BooleanValue::Get(kv.second);
		} else if (loption == "filename") {
			options.include_file_name = BooleanValue::Get(kv.second);
		} else if (loption == "hive_partitioning") {
			options.include_parsed_hive_partitions = BooleanValue::Get(kv.second);
		} else {
			options.SetReadOption(loption, kv.second, names);
		}
	}
	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified. Use read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		auto initial_reader = make_unique<BufferedCSVReader>(context, options);

		return_types.assign(initial_reader->sql_types.begin(), initial_reader->sql_types.end());
		if (names.empty()) {
			names.assign(initial_reader->col_names.begin(), initial_reader->col_names.end());
		} else {
			D_ASSERT(return_types.size() == names.size());
		}
		result->initial_reader = move(initial_reader);
	} else {
		result->sql_types = return_types;
		D_ASSERT(return_types.size() == names.size());
	}
	if (result->options.include_file_name) {
		result->filename_col_idx = names.size();
		return_types.emplace_back(LogicalType::VARCHAR);
		names.emplace_back("filename");
	}

	if (result->options.include_parsed_hive_partitions) {
		auto partitions = HivePartitioning::Parse(result->files[0]);
		result->hive_partition_col_idx = names.size();
		for (auto &part : partitions) {
			return_types.emplace_back(LogicalType::VARCHAR);
			names.emplace_back(part.first);
		}
	}
	result->options.names = names;
	return move(result);
}

struct ReadCSVOperatorData : public GlobalTableFunctionState {
	//! The CSV reader
	unique_ptr<BufferedCSVReader> csv_reader;
	//! The index of the next file to read (i.e. current file + 1)
	idx_t file_index;
	//! Total File Size
	idx_t file_size;
	//! How many bytes were read up to this point
	atomic<idx_t> bytes_read;
};

static unique_ptr<GlobalTableFunctionState> ReadCSVInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	auto result = make_unique<ReadCSVOperatorData>();
	if (bind_data.initial_reader) {
		result->csv_reader = move(bind_data.initial_reader);
	} else if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return move(result);
	} else {
		bind_data.options.file_path = bind_data.files[0];
		result->csv_reader = make_unique<BufferedCSVReader>(context, bind_data.options, bind_data.sql_types);
	}
	result->file_size = result->csv_reader->GetFileSize();
	result->file_index = 1;
	return move(result);
}

static unique_ptr<FunctionData> ReadCSVAutoBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	input.named_parameters["auto_detect"] = Value::BOOLEAN(true);
	return ReadCSVBind(context, input, return_types, names);
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*data_p.bind_data;
	auto &data = (ReadCSVOperatorData &)*data_p.global_state;

	if (!data.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}

	do {
		data.csv_reader->ParseCSV(output);
		data.bytes_read = data.csv_reader->bytes_in_chunk;
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

	if (bind_data.options.include_file_name) {
		auto &col = output.data[bind_data.filename_col_idx];
		col.SetValue(0, Value(data.csv_reader->options.file_path));
		col.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (bind_data.options.include_parsed_hive_partitions) {
		auto partitions = HivePartitioning::Parse(data.csv_reader->options.file_path);

		idx_t i = bind_data.hive_partition_col_idx;

		if (partitions.size() != (bind_data.options.names.size() - bind_data.hive_partition_col_idx)) {
			throw IOException("Hive partition count mismatch, expected " +
			                  std::to_string(bind_data.options.names.size() - bind_data.hive_partition_col_idx) +
			                  " hive partitions, got " + std::to_string(partitions.size()) + "\n");
		}

		for (auto &part : partitions) {
			if (bind_data.options.names[i] != part.first) {
				throw IOException("Hive partition names mismatch, expected '" + bind_data.options.names[i] +
				                  "' but found '" + part.first + "' for file '" + data.csv_reader->options.file_path +
				                  "'");
			}
			auto &col = output.data[i++];
			col.SetValue(0, Value(part.second));
			col.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}
}

static void ReadCSVAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["sample_chunk_size"] = LogicalType::BIGINT;
	table_function.named_parameters["sample_chunks"] = LogicalType::BIGINT;
	table_function.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["normalize_names"] = LogicalType::BOOLEAN;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;
	table_function.named_parameters["filename"] = LogicalType::BOOLEAN;
	table_function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	table_function.named_parameters["skip"] = LogicalType::BIGINT;
	table_function.named_parameters["max_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_line_size"] = LogicalType::VARCHAR;
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	auto &data = (const ReadCSVOperatorData &)*global_state;
	if (data.file_size == 0) {
		return 100;
	}
	auto percentage = (data.bytes_read * 100.0) / data.file_size;
	return percentage;
}

void CSVComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                              vector<unique_ptr<Expression>> &filters) {
	auto data = (ReadCSVData *)bind_data_p;

	if (data->options.include_parsed_hive_partitions || data->options.include_file_name) {
		string first_file = data->files[0];

		unordered_map<string, column_t> column_map;
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			column_map.insert({get.names[get.column_ids[i]], i});
		}

		HivePartitioning::ApplyFiltersToFileList(data->files, filters, column_map, get.table_index,
		                                         data->options.include_parsed_hive_partitions,
		                                         data->options.include_file_name);

		if (data->files.empty() || data->files[0] != first_file) {
			data->initial_reader.reset();
		}
	}
}

void BufferedCSVReaderOptions::Serialize(FieldWriter &writer) const {
	// common options
	writer.WriteField<bool>(has_delimiter);
	writer.WriteString(delimiter);
	writer.WriteField<bool>(has_quote);
	writer.WriteString(quote);
	writer.WriteField<bool>(has_escape);
	writer.WriteString(escape);
	writer.WriteField<bool>(has_header);
	writer.WriteField<bool>(header);
	writer.WriteField<bool>(ignore_errors);
	writer.WriteField<idx_t>(num_cols);
	writer.WriteField<idx_t>(buffer_size);
	writer.WriteString(null_str);
	writer.WriteField<FileCompressionType>(compression);
	// read options
	writer.WriteList<string>(names);
	writer.WriteField<idx_t>(skip_rows);
	writer.WriteField<idx_t>(maximum_line_size);
	writer.WriteField<bool>(normalize_names);
	writer.WriteListNoReference<bool>(force_not_null);
	writer.WriteField<bool>(all_varchar);
	writer.WriteField<idx_t>(sample_chunk_size);
	writer.WriteField<idx_t>(sample_chunks);
	writer.WriteField<bool>(auto_detect);
	writer.WriteString(file_path);
	writer.WriteField<bool>(include_file_name);
	writer.WriteField<bool>(include_parsed_hive_partitions);
	// write options
	writer.WriteListNoReference<bool>(force_quote);
}

void BufferedCSVReaderOptions::Deserialize(FieldReader &reader) {
	// common options
	has_delimiter = reader.ReadRequired<bool>();
	delimiter = reader.ReadRequired<string>();
	has_quote = reader.ReadRequired<bool>();
	quote = reader.ReadRequired<string>();
	has_escape = reader.ReadRequired<bool>();
	escape = reader.ReadRequired<string>();
	has_header = reader.ReadRequired<bool>();
	header = reader.ReadRequired<bool>();
	ignore_errors = reader.ReadRequired<bool>();
	num_cols = reader.ReadRequired<idx_t>();
	buffer_size = reader.ReadRequired<idx_t>();
	null_str = reader.ReadRequired<string>();
	compression = reader.ReadRequired<FileCompressionType>();
	// read options
	names = reader.ReadRequiredList<string>();
	skip_rows = reader.ReadRequired<idx_t>();
	maximum_line_size = reader.ReadRequired<idx_t>();
	normalize_names = reader.ReadRequired<bool>();
	force_not_null = reader.ReadRequiredList<bool>();
	all_varchar = reader.ReadRequired<bool>();
	sample_chunk_size = reader.ReadRequired<idx_t>();
	sample_chunks = reader.ReadRequired<idx_t>();
	auto_detect = reader.ReadRequired<bool>();
	file_path = reader.ReadRequired<string>();
	include_file_name = reader.ReadRequired<bool>();
	include_parsed_hive_partitions = reader.ReadRequired<bool>();
	// write options
	force_quote = reader.ReadRequiredList<bool>();
}

static void CSVReaderSerialize(FieldWriter &writer, const FunctionData *bind_data_p, const TableFunction &function) {
	auto &bind_data = (ReadCSVData &)*bind_data_p;
	writer.WriteList<string>(bind_data.files);
	writer.WriteRegularSerializableList<LogicalType>(bind_data.sql_types);
	writer.WriteField<idx_t>(bind_data.filename_col_idx);
	writer.WriteField<idx_t>(bind_data.hive_partition_col_idx);
	bind_data.options.Serialize(writer);
}

static unique_ptr<FunctionData> CSVReaderDeserialize(ClientContext &context, FieldReader &reader,
                                                     TableFunction &function) {
	auto result_data = make_unique<ReadCSVData>();
	result_data->files = reader.ReadRequiredList<string>();
	result_data->sql_types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	result_data->filename_col_idx = reader.ReadRequired<idx_t>();
	result_data->hive_partition_col_idx = reader.ReadRequired<idx_t>();
	result_data->options.Deserialize(reader);
	return move(result_data);
}

TableFunction ReadCSVTableFunction::GetFunction() {
	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, ReadCSVFunction, ReadCSVBind, ReadCSVInit);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ReadCSVTableFunction::GetFunction());

	TableFunction read_csv_auto("read_csv_auto", {LogicalType::VARCHAR}, ReadCSVFunction, ReadCSVAutoBind, ReadCSVInit);
	read_csv_auto.table_scan_progress = CSVReaderProgress;
	read_csv_auto.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv_auto.serialize = CSVReaderSerialize;
	read_csv_auto.deserialize = CSVReaderDeserialize;
	ReadCSVAddNamedParameters(read_csv_auto);
	set.AddFunction(read_csv_auto);
}

unique_ptr<TableFunctionRef> ReadCSVReplacement(ClientContext &context, const string &table_name,
                                                ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::EndsWith(lower_name, ".tsv")) {
		return nullptr;
	}
	auto table_function = make_unique<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(table_name)));
	table_function->function = make_unique<FunctionExpression>("read_csv_auto", move(children));
	return table_function;
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
	auto &config = DBConfig::GetConfig(context);
	config.replacement_scans.emplace_back(ReadCSVReplacement);
}

} // namespace duckdb
