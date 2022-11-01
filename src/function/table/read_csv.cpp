#include "duckdb/function/table/read_csv.hpp"
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

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const BufferedCSVReaderOptions &options, ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto opener = FileSystem::GetFileOpener(context);
	auto file_handle = fs.OpenFile(options.file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
	                               options.compression, opener);
	return make_unique<CSVFileHandle>(move(file_handle));
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning CSV files is disabled through configuration");
	}
	auto result = make_unique<ReadCSVData>();
	auto &options = result->options;

	vector<string> patterns;
	if (input.inputs[0].type().id() == LogicalTypeId::LIST) {
		// list of globs
		for (auto &val : ListValue::GetChildren(input.inputs[0])) {
			patterns.push_back(StringValue::Get(val));
		}
	} else {
		// single glob pattern
		patterns.push_back(StringValue::Get(input.inputs[0]));
	}

	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file_pattern : patterns) {
		auto files = fs.Glob(file_pattern, context);
		if (files.empty()) {
			throw IOException("No files found that match the pattern \"%s\"", file_pattern);
		}
		result->files.insert(result->files.end(), files.begin(), files.end());
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
		} else if (loption == "buffer_size") {
			options.buffer_size = kv.second.GetValue<uint64_t>();
			if (options.buffer_size == 0) {
				throw InvalidInputException("Buffer Size option must be higher than 0");
			}
		} else {
			options.SetReadOption(loption, kv.second, names);
		}
	}
	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		result->initial_file_handler = ReadCSV::OpenCSV(options, context);
		auto initial_reader = make_unique<BufferedCSVReader>(context, options, result->initial_file_handler.get());
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

	// union_col_names will exclude filename and hivepartition
	if (options.union_by_name) {
		idx_t union_names_index = 0;
		case_insensitive_map_t<idx_t> union_names_map;
		vector<string> union_col_names;
		vector<LogicalType> union_col_types;

		for (idx_t file_idx = 0; file_idx < result->files.size(); ++file_idx) {
			options.file_path = result->files[file_idx];
			auto handler = ReadCSV::OpenCSV(options, context);
			auto reader = make_unique<BufferedCSVReader>(context, options, handler.get());
			auto &col_names = reader->col_names;
			auto &sql_types = reader->sql_types;
			D_ASSERT(col_names.size() == sql_types.size());

			for (idx_t col = 0; col < col_names.size(); ++col) {
				auto union_find = union_names_map.find(col_names[col]);

				if (union_find != union_names_map.end()) {
					// given same name , union_col's type must compatible with col's type
					LogicalType compatible_type;
					compatible_type = LogicalType::MaxLogicalType(union_col_types[union_find->second], sql_types[col]);
					union_col_types[union_find->second] = compatible_type;
				} else {
					union_names_map[col_names[col]] = union_names_index;
					union_names_index++;

					union_col_names.emplace_back(col_names[col]);
					union_col_types.emplace_back(sql_types[col]);
				}
			}
			result->union_file_handlers.push_back(move(handler));
			result->union_readers.push_back(move(reader));
		}

		for (auto &reader : result->union_readers) {
			auto &col_names = reader->col_names;
			vector<bool> is_null_cols(union_col_names.size(), true);

			for (idx_t col = 0; col < col_names.size(); ++col) {
				idx_t remap_col = union_names_map[col_names[col]];
				reader->insert_cols_idx[col] = remap_col;
				is_null_cols[remap_col] = false;
			}
			for (idx_t col = 0; col < union_col_names.size(); ++col) {
				if (is_null_cols[col]) {
					reader->insert_nulls_idx.push_back(col);
				}
			}
		}

		const idx_t first_file_index = 0;
		result->initial_reader = move(result->union_readers[first_file_index]);

		names.assign(union_col_names.begin(), union_col_names.end());
		return_types.assign(union_col_types.begin(), union_col_types.end());
		D_ASSERT(names.size() == return_types.size());
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

static unique_ptr<GlobalTableFunctionState> ReadCSVInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (ReadCSVData &)*input.bind_data;
	// FIXME: Should I still care about this case?
	//	if (bind_data.initial_reader) {
	//		result->csv_reader = move(bind_data.initial_reader);
	//	}
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return make_unique<ReadCSVGlobalState>();
	}

	bind_data.options.file_path = bind_data.files[0];
	auto file_handle = ReadCSV::OpenCSV(bind_data.options, context);
	return make_unique<ReadCSVGlobalState>(move(file_handle), bind_data.files, context.db->NumberOfThreads(),
	                                       bind_data.options.buffer_size);
}

idx_t ReadCSVGlobalState::MaxThreads() const {
	//	idx_t one_mb = 1000000;
	//	idx_t threads_per_mb = first_file_size / one_mb + 1;
	//	if (threads_per_mb < system_threads) {
	//		return threads_per_mb;
	//	}
	return system_threads;
}

bool ReadCSVGlobalState::Finished() {
	lock_guard<mutex> parallel_lock(main_mutex);
	return !current_buffer;
}

unique_ptr<CSVBufferRead> ReadCSVGlobalState::Next(ClientContext &context, ReadCSVData &bind_data) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (!current_buffer) {
		// We are done scanning.
		return nullptr;
	}
	// set up the current buffer
	auto result =
	    make_unique<CSVBufferRead>(current_buffer, next_byte, next_byte + bytes_per_local_state, batch_index++);
	// move the byte index of the CSV reader to the next buffer
	next_byte += bytes_per_local_state;
	if (next_byte >= current_buffer->GetBufferSize()) {
		// We replace the current buffer with the next buffer
		next_byte = 0;
		current_buffer = next_buffer;
		if (next_buffer) {
			// Next buffer gets the next-next buffer
			next_buffer = next_buffer->Next(*file_handle, buffer_size);
		}
	}
	if (current_buffer && !next_buffer) {
		// This means we are done with the current file, we need to go to the next one (if exists).
		if (file_index < bind_data.files.size()) {
			bind_data.options.file_path = bind_data.files[file_index++];
			file_handle = ReadCSV::OpenCSV(bind_data.options, context);
			next_buffer = make_shared<CSVBuffer>(buffer_size, *file_handle);
		}
	}
	return result;
}

unique_ptr<LocalTableFunctionState> ReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p) {
	auto &global_state = (ReadCSVGlobalState &)*global_state_p;
	auto &csv_data = (ReadCSVData &)*input.bind_data;
	auto next_local_buffer = global_state.Next(context.client, csv_data);
	unique_ptr<BufferedCSVReader> csv_reader;
	if (next_local_buffer) {
		bool single_threaded = global_state.MaxThreads() == 1;
		csv_reader = make_unique<BufferedCSVReader>(context.client, csv_data.options, *next_local_buffer,
		                                            single_threaded, csv_data.sql_types);
	}
	auto new_local_state = make_unique<ReadCSVLocalState>(move(csv_reader));
	return move(new_local_state);
}

static unique_ptr<FunctionData> ReadCSVAutoBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	input.named_parameters["auto_detect"] = Value::BOOLEAN(true);
	return ReadCSVBind(context, input, return_types, names);
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (ReadCSVData &)*data_p.bind_data;
	auto &csv_global_state = (ReadCSVGlobalState &)*data_p.global_state;
	auto &csv_local_state = (ReadCSVLocalState &)*data_p.local_state;

	if (!csv_local_state.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}

	do {
		if (output.size() != 0 || (csv_global_state.Finished() && csv_local_state.csv_reader->position_buffer >=
		                                                              csv_local_state.csv_reader->end_buffer)) {
			break;
		}
		if (csv_local_state.csv_reader->position_buffer >= csv_local_state.csv_reader->end_buffer) {
			auto next_chunk = csv_global_state.Next(context, bind_data);
			if (!next_chunk) {
				break;
			}
			csv_local_state.csv_reader->SetBufferRead(*next_chunk);
		}
		csv_local_state.csv_reader->ParseCSV(output);

	} while (true);

	if (bind_data.options.union_by_name) {
		D_ASSERT(0);
		//		csv_global_state.csv_reader->SetNullUnionCols(output);
	}
	if (bind_data.options.include_file_name) {
		D_ASSERT(0);
		//		auto &col = output.data[bind_data.filename_col_idx];
		//		col.SetValue(0, Value(csv_global_state.csv_reader->options.file_path));
		//		col.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (bind_data.options.include_parsed_hive_partitions) {
		D_ASSERT(0);
		//		auto partitions = HivePartitioning::Parse(csv_global_state.csv_reader->options.file_path);
		//
		//		idx_t i = bind_data.hive_partition_col_idx;
		//
		//		if (partitions.size() != (bind_data.options.names.size() - bind_data.hive_partition_col_idx)) {
		//			throw IOException("Hive partition count mismatch, expected " +
		//			                  std::to_string(bind_data.options.names.size() - bind_data.hive_partition_col_idx)
		//+ 			                  " hive partitions, got " + std::to_string(partitions.size()) + "\n");
		//		}
		//
		//		for (auto &part : partitions) {
		//			if (bind_data.options.names[i] != part.first) {
		//				throw IOException("Hive partition names mismatch, expected '" + bind_data.options.names[i] +
		//				                  "' but found '" + part.first + "' for file '" +
		//				                  csv_global_state.csv_reader->options.file_path + "'");
		//			}
		//			auto &col = output.data[i++];
		//			col.SetValue(0, Value(part.second));
		//			col.SetVectorType(VectorType::CONSTANT_VECTOR);
		//		}
	}
}

static idx_t CSVReaderGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &data = (ReadCSVLocalState &)*local_state;
	return data.csv_reader->buffer_read.batch_index;
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
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	table_function.named_parameters["buffer_size"] = LogicalType::UBIGINT;
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	//	auto &data = (const ReadCSVGlobalState &)*global_state;
	return 100;
	//	if (data.file_size == 0) {
	//		return 100;
	//	}
	//	auto percentage = (data.bytes_read * 100.0) / data.file_size;
	//	return percentage;
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
	writer.WriteField<idx_t>(buffer_sample_size);
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
	buffer_sample_size = reader.ReadRequired<idx_t>();
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

TableFunction ReadCSVTableFunction::GetFunction(bool list_parameter) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction read_csv("read_csv", {parameter}, ReadCSVFunction, ReadCSVBind, ReadCSVInitGlobal, ReadCSVInitLocal);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.get_batch_index = CSVReaderGetBatchIndex;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetAutoFunction(bool list_parameter) {
	auto parameter = list_parameter ? LogicalType::LIST(LogicalType::VARCHAR) : LogicalType::VARCHAR;
	TableFunction read_csv_auto("read_csv_auto", {parameter}, ReadCSVFunction, ReadCSVAutoBind, ReadCSVInitGlobal,
	                            ReadCSVInitLocal);
	read_csv_auto.table_scan_progress = CSVReaderProgress;
	read_csv_auto.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv_auto.serialize = CSVReaderSerialize;
	read_csv_auto.deserialize = CSVReaderDeserialize;
	read_csv_auto.get_batch_index = CSVReaderGetBatchIndex;
	ReadCSVAddNamedParameters(read_csv_auto);
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet read_csv("read_csv");
	read_csv.AddFunction(ReadCSVTableFunction::GetFunction());
	read_csv.AddFunction(ReadCSVTableFunction::GetFunction(true));
	set.AddFunction(read_csv);

	TableFunctionSet read_csv_auto("read_csv_auto");
	read_csv_auto.AddFunction(ReadCSVTableFunction::GetAutoFunction());
	read_csv_auto.AddFunction(ReadCSVTableFunction::GetAutoFunction(true));
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
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::Contains(lower_name, ".csv?") &&
	    !StringUtil::EndsWith(lower_name, ".tsv") && !StringUtil::Contains(lower_name, ".tsv?")) {
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
