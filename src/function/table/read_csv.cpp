#include "duckdb/function/table/read_csv.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/union_by_name.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/execution/operator/scan/csv/csv_line_info.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/execution/operator/scan/csv/table_function/global_csv_state.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <limits>

namespace duckdb {

unique_ptr<CSVFileHandle> ReadCSV::OpenCSV(const string &file_path, FileCompressionType compression,
                                           ClientContext &context) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &allocator = BufferAllocator::Get(context);
	return CSVFileHandle::OpenFile(fs, allocator, file_path, compression);
}

void ReadCSVData::FinalizeRead(ClientContext &context) {
	BaseCSVData::Finalize();
	// We can't parallelize files with a mix of new line delimiters, or with null padding options
	// Since we won't be able to detect where new lines start correctly
	bool not_supported_options = options.null_padding || options.dialect_options.new_line == NewLineIdentifier::MIX;

	auto number_of_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	// If we have many csv files, we run single-threaded on each file and parallelize on the number of files
	bool many_csv_files = files.size() > 1 && int64_t(files.size() * 2) >= number_of_threads;
	if (many_csv_files || not_supported_options) {
		// not supported for parallel CSV reading
		parallelize_single_file_scan = false;
	}

	if (!options.rejects_recovery_columns.empty()) {
		for (auto &recovery_col : options.rejects_recovery_columns) {
			bool found = false;
			for (idx_t col_idx = 0; col_idx < return_names.size(); col_idx++) {
				if (StringUtil::CIEquals(return_names[col_idx], recovery_col)) {
					options.rejects_recovery_column_ids.push_back(col_idx);
					found = true;
					break;
				}
			}
			if (!found) {
				throw BinderException("Unsupported parameter for REJECTS_RECOVERY_COLUMNS: column \"%s\" not found",
				                      recovery_col);
			}
		}
	}
	state_machine = state_machine_cache.Get(options.dialect_options.state_machine_options);
}

static unique_ptr<FunctionData> ReadCSVBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<ReadCSVData>();
	auto &options = result->options;
	result->files = MultiFileReader::GetFileList(context, input.inputs[0], "CSV");

	options.FromNamedParameters(input.named_parameters, context, return_types, names);

	// Validate rejects_table options
	if (!options.rejects_table_name.empty()) {
		if (!options.ignore_errors) {
			throw BinderException("REJECTS_TABLE option is only supported when IGNORE_ERRORS is set to true");
		}
		if (options.file_options.union_by_name) {
			throw BinderException("REJECTS_TABLE option is not supported when UNION_BY_NAME is set to true");
		}
	}

	if (options.rejects_limit != 0) {
		if (options.rejects_table_name.empty()) {
			throw BinderException("REJECTS_LIMIT option is only supported when REJECTS_TABLE is set to a table name");
		}
	}

	if (!options.rejects_recovery_columns.empty() && options.rejects_table_name.empty()) {
		throw BinderException(
		    "REJECTS_RECOVERY_COLUMNS option is only supported when REJECTS_TABLE is set to a table name");
	}

	options.file_options.AutoDetectHivePartitioning(result->files, context);

	if (!options.auto_detect && return_types.empty()) {
		throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
		                      "read_csv_auto or set read_csv(..., "
		                      "AUTO_DETECT=TRUE) to automatically guess columns.");
	}
	if (options.auto_detect) {
		options.file_path = result->files[0];
		result->buffer_manager = make_shared<CSVBufferManager>(context, options, result->files);
		CSVSniffer sniffer(options, result->buffer_manager, result->state_machine_cache, {&return_types, &names});
		auto sniffer_result = sniffer.SniffCSV();
		if (names.empty()) {
			names = sniffer_result.names;
			return_types = sniffer_result.return_types;
		}
	}

	D_ASSERT(return_types.size() == names.size());

	result->csv_types = return_types;
	result->csv_names = names;

	if (options.file_options.union_by_name) {
		D_ASSERT(0);
		//		result->reader_bind =
		//		    MultiFileReader::BindUnionReader<CSVScanner>(context, return_types, names, *result, options);
		//		if (result->union_readers.size() > 1) {
		//			result->column_info.emplace_back(result->csv_names, result->csv_types);
		//			for (idx_t i = 1; i < result->union_readers.size(); i++) {
		//				result->column_info.emplace_back(result->union_readers[i]->names,
		//				                                 result->union_readers[i]->return_types);
		//			}
		//		}
		//		if (!options.sql_types_per_column.empty()) {
		//			auto exception = CSVScanner::ColumnTypesError(options.sql_types_per_column, names);
		//			if (!exception.empty()) {
		//				throw BinderException(exception);
		//			}
		//		}
	} else {
		result->reader_bind = MultiFileReader::BindOptions(options.file_options, result->files, return_types, names);
	}
	result->return_types = return_types;
	result->return_names = names;
	result->FinalizeRead(context);
	result->options.dialect_options.num_cols = names.size();
	return std::move(result);
}

void LineInfo::Verify(idx_t file_idx, idx_t batch_idx, idx_t cur_first_pos) {
	//	auto &tuple_start_set = tuple_start[file_idx];
	//	auto &processed_batches = batch_to_tuple_end[file_idx];
	//	auto &tuple_end_vec = tuple_end[file_idx];
	//	bool has_error = false;
	//	idx_t problematic_line;
	//	if (batch_idx == 0 || tuple_start_set.empty()) {
	//		return;
	//	}
	//	for (idx_t cur_batch = 0; cur_batch < batch_idx - 1; cur_batch++) {
	//		auto cur_end = tuple_end_vec[processed_batches[cur_batch]];
	//		auto first_pos = tuple_start_set.find(cur_end);
	//		if (first_pos == tuple_start_set.end()) {
	//			has_error = true;
	//			problematic_line = GetLine(cur_batch);
	//			break;
	//		}
	//	}
	//	if (!has_error) {
	//		auto cur_end = tuple_end_vec[processed_batches[batch_idx - 1]];
	//		if (cur_end != cur_first_pos) {
	//			has_error = true;
	//			problematic_line = GetLine(batch_idx);
	//		}
	//	}
	//	if (has_error) {
	//		throw InvalidInputException(
	//		    "CSV File not supported for multithreading. This can be a problematic line in your CSV File or "
	//		    "that this CSV can't be read in Parallel. Please, inspect if the line %llu is correct. If so, "
	//		    "please run single-threaded CSV Reading by setting parallel=false in the read_csv call.\n %s",
	//		    problematic_line, sniffer_mismatch_error);
	//	}
}

bool LineInfo::CanItGetLine(idx_t file_idx, idx_t batch_idx) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (current_batches.empty() || done) {
		return true;
	}
	if (file_idx >= current_batches.size() || current_batches[file_idx].empty()) {
		return true;
	}
	auto min_value = *current_batches[file_idx].begin();
	if (min_value >= batch_idx) {
		return true;
	}
	return false;
}

void LineInfo::Increment(idx_t file_idx, idx_t batch_idx) {
	auto parallel_lock = duckdb::make_uniq<lock_guard<mutex>>(main_mutex);
	lines_errored[file_idx][batch_idx]++;
}

// Returns the 1-indexed line number
idx_t LineInfo::GetLine(idx_t batch_idx, idx_t line_error, idx_t file_idx, idx_t cur_start, bool verify,
                        bool stop_at_first) {
	unique_ptr<lock_guard<mutex>> parallel_lock;
	if (!verify) {
		parallel_lock = duckdb::make_uniq<lock_guard<mutex>>(main_mutex);
	}
	idx_t line_count = 0;

	if (!stop_at_first) {
		// Figure out the amount of lines read in the current file
		for (idx_t cur_batch_idx = 0; cur_batch_idx <= batch_idx; cur_batch_idx++) {
			if (cur_batch_idx < batch_idx) {
				line_count += lines_errored[file_idx][cur_batch_idx];
			}
			line_count += lines_read[file_idx][cur_batch_idx];
		}
		return line_count + line_error + 1;
	}

	// Otherwise, check if we already have an error on another thread
	if (done) {
		// line count is 0-indexed, but we want to return 1-indexed
		return first_line + 1;
	}
	for (idx_t i = 0; i <= batch_idx; i++) {
		if (lines_read[file_idx].find(i) == lines_read[file_idx].end() && i != batch_idx) {
			throw InternalException("Missing batch index on Parallel CSV Reader GetLine");
		}
		line_count += lines_read[file_idx][i];
	}

	// before we are done, if this is not a call in Verify() we must check Verify up to this batch
	if (!verify) {
		Verify(file_idx, batch_idx, cur_start);
	}
	done = true;
	first_line = line_count + line_error;
	// line count is 0-indexed, but we want to return 1-indexed
	return first_line + 1;
}

//===--------------------------------------------------------------------===//
// Read CSV Local State
//===--------------------------------------------------------------------===//
struct CSVLocalState : public LocalTableFunctionState {
public:
	explicit CSVLocalState(unique_ptr<StringValueScanner> csv_reader_p) : csv_reader(std::move(csv_reader_p)) {
	}

	//! The CSV reader
	unique_ptr<StringValueScanner> csv_reader;
	bool done = false;
};

//===--------------------------------------------------------------------===//
// Read CSV Functions
//===--------------------------------------------------------------------===//
static unique_ptr<GlobalTableFunctionState> ReadCSVInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadCSVData>();

	// Create the temporary rejects table
	auto rejects_table = bind_data.options.rejects_table_name;
	if (!rejects_table.empty()) {
		CSVRejectsTable::GetOrCreate(context, rejects_table)->InitializeTable(context, bind_data);
	}
	if (bind_data.files.empty()) {
		// This can happen when a filename based filter pushdown has eliminated all possible files for this scan.
		return nullptr;
	}
	return make_uniq<CSVGlobalState>(context, bind_data.buffer_manager, bind_data.options,
	                                 context.db->NumberOfThreads(), bind_data.files, input.column_ids,
	                                 bind_data.state_machine);
}

unique_ptr<LocalTableFunctionState> ReadCSVInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p) {
	auto &csv_data = input.bind_data->Cast<ReadCSVData>();
	auto &global_state = global_state_p->Cast<CSVGlobalState>();
	auto csv_scanner = global_state.Next(context.client, csv_data, global_state.current_boundary);
	if (!csv_scanner) {
		global_state.DecrementThread();
	}
	return make_uniq<CSVLocalState>(std::move(csv_scanner));
}

static void ReadCSVFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ReadCSVData>();
	auto &csv_global_state = data_p.global_state->Cast<CSVGlobalState>();
	auto &csv_local_state = data_p.local_state->Cast<CSVLocalState>();

	if (!csv_local_state.csv_reader) {
		// no csv_reader was set, this can happen when a filename-based filter has filtered out all possible files
		return;
	}
	do {
		if (output.size() != 0) {
			//			MultiFileReader::FinalizeChunk(bind_data.reader_bind,
			//			                               csv_local_state.csv_reader->reader_data,
			//			                               output);
			break;
		}
		if (csv_local_state.csv_reader->Finished()) {

			//			auto verification_updates = csv_local_state.csv_reader->GetVerificationPositions();
			//			csv_global_state.UpdateVerification(verification_updates,
			//			 csv_local_state.csv_reader->file_idx,
			//									                                    csv_local_state.csv_reader->scanner->scanner_id);
			//			                        csv_global_state.UpdateLinesRead(*csv_local_state.csv_reader->scanner,
			//			                                 csv_local_state.csv_reader->file_idx);
			csv_local_state.csv_reader =
			    csv_global_state.Next(context, bind_data, csv_local_state.csv_reader->GetIterator());
			//			if (csv_local_state.csv_reader) {
			//				csv_local_state.csv_reader->linenr = 0;
			//			}
			if (!csv_local_state.csv_reader) {
				csv_global_state.DecrementThread();
				break;
			}
		}
		//		VerificationPositions positions;
		csv_local_state.csv_reader->Flush(output);

	} while (true);
}

static idx_t CSVReaderGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                    LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &data = local_state->Cast<CSVLocalState>();
	return data.csv_reader->GetBoundaryIndex();
}

void ReadCSVTableFunction::ReadCSVAddNamedParameters(TableFunction &table_function) {
	table_function.named_parameters["sep"] = LogicalType::VARCHAR;
	table_function.named_parameters["delim"] = LogicalType::VARCHAR;
	table_function.named_parameters["quote"] = LogicalType::VARCHAR;
	table_function.named_parameters["new_line"] = LogicalType::VARCHAR;
	table_function.named_parameters["escape"] = LogicalType::VARCHAR;
	table_function.named_parameters["nullstr"] = LogicalType::VARCHAR;
	table_function.named_parameters["columns"] = LogicalType::ANY;
	table_function.named_parameters["auto_type_candidates"] = LogicalType::ANY;
	table_function.named_parameters["header"] = LogicalType::BOOLEAN;
	table_function.named_parameters["auto_detect"] = LogicalType::BOOLEAN;
	table_function.named_parameters["sample_size"] = LogicalType::BIGINT;
	table_function.named_parameters["all_varchar"] = LogicalType::BOOLEAN;
	table_function.named_parameters["dateformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["timestampformat"] = LogicalType::VARCHAR;
	table_function.named_parameters["normalize_names"] = LogicalType::BOOLEAN;
	table_function.named_parameters["compression"] = LogicalType::VARCHAR;
	table_function.named_parameters["skip"] = LogicalType::BIGINT;
	table_function.named_parameters["max_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["maximum_line_size"] = LogicalType::VARCHAR;
	table_function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	table_function.named_parameters["rejects_table"] = LogicalType::VARCHAR;
	table_function.named_parameters["rejects_limit"] = LogicalType::BIGINT;
	table_function.named_parameters["rejects_recovery_columns"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["buffer_size"] = LogicalType::UBIGINT;
	table_function.named_parameters["decimal_separator"] = LogicalType::VARCHAR;
	table_function.named_parameters["parallel"] = LogicalType::BOOLEAN;
	table_function.named_parameters["null_padding"] = LogicalType::BOOLEAN;
	table_function.named_parameters["allow_quoted_nulls"] = LogicalType::BOOLEAN;
	table_function.named_parameters["column_types"] = LogicalType::ANY;
	table_function.named_parameters["dtypes"] = LogicalType::ANY;
	table_function.named_parameters["types"] = LogicalType::ANY;
	table_function.named_parameters["names"] = LogicalType::LIST(LogicalType::VARCHAR);
	table_function.named_parameters["column_names"] = LogicalType::LIST(LogicalType::VARCHAR);
	MultiFileReader::AddParameters(table_function);
}

double CSVReaderProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	auto &data = global_state->Cast<CSVGlobalState>();
	return data.GetProgress(bind_data);
}

void CSVComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                              vector<unique_ptr<Expression>> &filters) {
	auto &data = bind_data_p->Cast<ReadCSVData>();
	auto reset_reader =
	    MultiFileReader::ComplexFilterPushdown(context, data.files, data.options.file_options, get, filters);
	if (reset_reader) {
		D_ASSERT(0);
		//		MultiFileReader::PruneReaders(data);
	}
}

unique_ptr<NodeStatistics> CSVReaderCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	idx_t per_file_cardinality = 0;
	if (bind_data.buffer_manager && bind_data.buffer_manager->file_handle) {
		auto estimated_row_width = (bind_data.csv_types.size() * 5);
		per_file_cardinality = bind_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
	} else {
		// determined through the scientific method as the average amount of rows in a CSV file
		per_file_cardinality = 42;
	}
	return make_uniq<NodeStatistics>(bind_data.files.size() * per_file_cardinality);
}

static void CSVReaderSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<ReadCSVData>();
	serializer.WriteProperty(100, "extra_info", function.extra_info);
	serializer.WriteProperty(101, "csv_data", &bind_data);
}

static unique_ptr<FunctionData> CSVReaderDeserialize(Deserializer &deserializer, TableFunction &function) {
	unique_ptr<ReadCSVData> result;
	deserializer.ReadProperty(100, "extra_info", function.extra_info);
	deserializer.ReadProperty(101, "csv_data", result);
	return std::move(result);
}

TableFunction ReadCSVTableFunction::GetFunction() {
	TableFunction read_csv("read_csv", {LogicalType::VARCHAR}, ReadCSVFunction, ReadCSVBind, ReadCSVInitGlobal,
	                       ReadCSVInitLocal);
	read_csv.table_scan_progress = CSVReaderProgress;
	read_csv.pushdown_complex_filter = CSVComplexFilterPushdown;
	read_csv.serialize = CSVReaderSerialize;
	read_csv.deserialize = CSVReaderDeserialize;
	read_csv.get_batch_index = CSVReaderGetBatchIndex;
	read_csv.cardinality = CSVReaderCardinality;
	read_csv.projection_pushdown = true;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetAutoFunction() {
	auto read_csv_auto = ReadCSVTableFunction::GetFunction();
	read_csv_auto.name = "read_csv_auto";
	read_csv_auto.bind = ReadCSVBind;
	return read_csv_auto;
}

void ReadCSVTableFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetFunction()));
	set.AddFunction(MultiFileReader::CreateFunctionSet(ReadCSVTableFunction::GetAutoFunction()));
}

unique_ptr<TableRef> ReadCSVReplacement(ClientContext &context, const string &table_name, ReplacementScanData *data) {
	auto lower_name = StringUtil::Lower(table_name);
	// remove any compression
	if (StringUtil::EndsWith(lower_name, ".gz")) {
		lower_name = lower_name.substr(0, lower_name.size() - 3);
	} else if (StringUtil::EndsWith(lower_name, ".zst")) {
		if (!Catalog::TryAutoLoad(context, "parquet")) {
			throw MissingExtensionException("parquet extension is required for reading zst compressed file");
		}
		lower_name = lower_name.substr(0, lower_name.size() - 4);
	}
	if (!StringUtil::EndsWith(lower_name, ".csv") && !StringUtil::Contains(lower_name, ".csv?") &&
	    !StringUtil::EndsWith(lower_name, ".tsv") && !StringUtil::Contains(lower_name, ".tsv?")) {
		return nullptr;
	}
	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	if (!FileSystem::HasGlob(table_name)) {
		auto &fs = FileSystem::GetFileSystem(context);
		table_function->alias = fs.ExtractBaseName(table_name);
	}

	return std::move(table_function);
}

void BuiltinFunctions::RegisterReadFunctions() {
	CSVCopyFunction::RegisterFunction(*this);
	ReadCSVTableFunction::RegisterFunction(*this);
	auto &config = DBConfig::GetConfig(*transaction.db);
	config.replacement_scans.emplace_back(ReadCSVReplacement);
}

} // namespace duckdb
