#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_buffer.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/common/bind_helpers.hpp"

namespace duckdb {

unique_ptr<BaseFileReaderOptions> CSVMultiFileInfo::InitializeOptions(ClientContext &context,
                                                                      optional_ptr<TableFunctionInfo> info) {
	return make_uniq<CSVFileReaderOptions>();
}

bool CSVMultiFileInfo::ParseCopyOption(ClientContext &context, const string &key, const vector<Value> &values,
                                       BaseFileReaderOptions &options_p, vector<string> &expected_names,
                                       vector<LogicalType> &expected_types) {
	auto &options = options_p.Cast<CSVFileReaderOptions>();
	options.options.SetReadOption(StringUtil::Lower(key), ConvertVectorToValue(values), expected_names);
	return true;
}

bool CSVMultiFileInfo::ParseOption(ClientContext &context, const string &key, const Value &val,
                                   MultiFileReaderOptions &file_options, BaseFileReaderOptions &options_p) {
	auto &options = options_p.Cast<CSVFileReaderOptions>();
	options.options.ParseOption(context, key, val);
	return true;
}

void CSVMultiFileInfo::FinalizeCopyBind(ClientContext &context, BaseFileReaderOptions &options_p,
                                        const vector<string> &expected_names,
                                        const vector<LogicalType> &expected_types) {
	auto &options = options_p.Cast<CSVFileReaderOptions>();
	auto &csv_options = options.options;
	csv_options.name_list = expected_names;
	csv_options.sql_type_list = expected_types;
	csv_options.columns_set = true;
	for (idx_t i = 0; i < expected_types.size(); i++) {
		csv_options.sql_types_per_column[expected_names[i]] = i;
	}
}

unique_ptr<TableFunctionData> CSVMultiFileInfo::InitializeBindData(MultiFileBindData &multi_file_data,
                                                                   unique_ptr<BaseFileReaderOptions> options_p) {
	auto &options = options_p->Cast<CSVFileReaderOptions>();
	auto csv_data = make_uniq<ReadCSVData>();
	csv_data->options = std::move(options.options);
	if (multi_file_data.file_list->GetExpandResult() == FileExpandResult::MULTIPLE_FILES) {
		csv_data->options.multi_file_reader = true;
	}
	csv_data->options.Verify(multi_file_data.file_options);
	return std::move(csv_data);
}

//! Function to do schema discovery over one CSV file or a list/glob of CSV files
void SchemaDiscovery(ClientContext &context, ReadCSVData &result, CSVReaderOptions &options,
                     const MultiFileReaderOptions &file_options, vector<LogicalType> &return_types,
                     vector<string> &names, MultiFileList &multi_file_list) {
	vector<CSVSchema> schemas;
	const auto option_og = options;

	const auto file_paths = multi_file_list.GetAllFiles();

	// Here what we want to do is to sniff a given number of lines, if we have many files, we might go through them
	// to reach the number of lines.
	const idx_t required_number_of_lines = options.sniff_size * options.sample_size_chunks;

	idx_t total_number_of_rows = 0;
	idx_t current_file = 0;
	options.file_path = file_paths[current_file];

	result.buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, options.file_path, false);
	idx_t only_header_or_empty_files = 0;

	{
		CSVSniffer sniffer(options, file_options, result.buffer_manager, CSVStateMachineCache::Get(context));
		auto sniffer_result = sniffer.SniffCSV();
		idx_t rows_read = sniffer.LinesSniffed() -
		                  (options.dialect_options.skip_rows.GetValue() + options.dialect_options.header.GetValue());

		schemas.emplace_back(sniffer_result.names, sniffer_result.return_types, file_paths[0], rows_read,
		                     result.buffer_manager->GetBuffer(0)->actual_size == 0);
		total_number_of_rows += sniffer.LinesSniffed();
		current_file++;
		if (sniffer.EmptyOrOnlyHeader()) {
			only_header_or_empty_files++;
		}
	}

	// We do a copy of the options to not pollute the options of the first file.
	constexpr idx_t max_files_to_sniff = 10;
	idx_t files_to_sniff = file_paths.size() > max_files_to_sniff ? max_files_to_sniff : file_paths.size();
	while (total_number_of_rows < required_number_of_lines && current_file < files_to_sniff) {
		auto option_copy = option_og;
		option_copy.file_path = file_paths[current_file];
		auto buffer_manager = make_shared_ptr<CSVBufferManager>(context, option_copy, option_copy.file_path, false);
		// TODO: We could cache the sniffer to be reused during scanning. Currently that's an exercise left to the
		// reader
		CSVSniffer sniffer(option_copy, file_options, buffer_manager, CSVStateMachineCache::Get(context));
		auto sniffer_result = sniffer.SniffCSV();
		idx_t rows_read = sniffer.LinesSniffed() - (option_copy.dialect_options.skip_rows.GetValue() +
		                                            option_copy.dialect_options.header.GetValue());
		if (buffer_manager->GetBuffer(0)->actual_size == 0) {
			schemas.emplace_back(true);
		} else {
			schemas.emplace_back(sniffer_result.names, sniffer_result.return_types, option_copy.file_path, rows_read);
		}
		total_number_of_rows += sniffer.LinesSniffed();
		if (sniffer.EmptyOrOnlyHeader()) {
			only_header_or_empty_files++;
		}
		current_file++;
	}

	// We might now have multiple schemas, we need to go through them to define the one true schema
	CSVSchema best_schema;
	for (auto &schema : schemas) {
		if (best_schema.Empty()) {
			// A schema is bettah than no schema
			best_schema = schema;
		} else if (best_schema.GetRowsRead() == 0) {
			// If the best-schema has no data-rows, that's easy, we just take the new schema
			best_schema = schema;
		} else if (schema.GetRowsRead() != 0) {
			// We might have conflicting-schemas, we must merge them
			best_schema.MergeSchemas(schema, options.null_padding);
		}
	}

	if (names.empty()) {
		names = best_schema.GetNames();
		return_types = best_schema.GetTypes();
	}
	if (only_header_or_empty_files == current_file && !options.columns_set) {
		for (auto &type : return_types) {
			D_ASSERT(type.id() == LogicalTypeId::BOOLEAN);
			// we default to varchar if all files are empty or only have a header after all the sniffing
			type = LogicalType::VARCHAR;
		}
	}
	result.csv_schema = best_schema;
}

void CSVMultiFileInfo::BindReader(ClientContext &context, vector<LogicalType> &return_types, vector<string> &names,
                                  MultiFileBindData &bind_data) {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	auto &multi_file_list = *bind_data.file_list;
	auto &options = csv_data.options;
	if (!bind_data.file_options.union_by_name) {
		if (options.auto_detect) {
			SchemaDiscovery(context, csv_data, options, bind_data.file_options, return_types, names, multi_file_list);
		} else {
			// If we are not running the sniffer, the columns must be set!
			if (!options.columns_set) {
				throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
				                      "read_csv_auto or set read_csv(..., "
				                      "AUTO_DETECT=TRUE) to automatically guess columns.");
			}
			names = options.name_list;
			return_types = options.sql_type_list;
		}
		D_ASSERT(return_types.size() == names.size());
		csv_data.options.dialect_options.num_cols = names.size();

		bind_data.multi_file_reader->BindOptions(bind_data.file_options, multi_file_list, return_types, names,
		                                         bind_data.reader_bind);
	} else {
		bind_data.reader_bind = bind_data.multi_file_reader->BindUnionReader<CSVMultiFileInfo>(
		    context, return_types, names, multi_file_list, bind_data, options, bind_data.file_options);
		if (bind_data.union_readers.size() > 1) {
			for (idx_t i = 0; i < bind_data.union_readers.size(); i++) {
				auto &csv_union_data = bind_data.union_readers[i]->Cast<CSVUnionData>();
				csv_data.column_info.emplace_back(csv_union_data.names, csv_union_data.types);
			}
		}
		if (!options.sql_types_per_column.empty()) {
			const auto exception = CSVError::ColumnTypesError(options.sql_types_per_column, names);
			if (!exception.error_message.empty()) {
				throw BinderException(exception.error_message);
			}
			for (idx_t i = 0; i < names.size(); i++) {
				auto it = options.sql_types_per_column.find(names[i]);
				if (it != options.sql_types_per_column.end()) {
					return_types[i] = options.sql_type_list[it->second];
				}
			}
		}
		bind_data.initial_reader.reset();
	}
}

void CSVMultiFileInfo::FinalizeBindData(MultiFileBindData &multi_file_data) {
	auto &csv_data = multi_file_data.bind_data->Cast<ReadCSVData>();
	auto &names = multi_file_data.names;
	auto &options = csv_data.options;
	if (!options.force_not_null_names.empty()) {
		// Let's first check all column names match
		duckdb::unordered_set<string> column_names;
		for (auto &name : names) {
			column_names.insert(name);
		}
		for (auto &force_name : options.force_not_null_names) {
			if (column_names.find(force_name) == column_names.end()) {
				throw BinderException("\"force_not_null\" expected to find %s, but it was not found in the table",
				                      force_name);
			}
		}
		D_ASSERT(options.force_not_null.empty());
		for (idx_t i = 0; i < names.size(); i++) {
			if (options.force_not_null_names.find(names[i]) != options.force_not_null_names.end()) {
				options.force_not_null.push_back(true);
			} else {
				options.force_not_null.push_back(false);
			}
		}
	}
	csv_data.Finalize();
}

void CSVMultiFileInfo::GetBindInfo(const TableFunctionData &bind_data, BindInfo &info) {
}

optional_idx CSVMultiFileInfo::MaxThreads(const MultiFileBindData &bind_data, const MultiFileGlobalState &global_state,
                                          FileExpandResult expand_result) {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	if (!csv_data.buffer_manager) {
		return optional_idx();
	}
	if (expand_result == FileExpandResult::MULTIPLE_FILES) {
		// always launch max threads if we are reading multiple files
		return optional_idx();
	}
	const idx_t bytes_per_thread = CSVIterator::BytesPerThread(csv_data.options);
	const idx_t file_size = csv_data.buffer_manager->file_handle->FileSize();
	return file_size / bytes_per_thread + 1;
}

unique_ptr<GlobalTableFunctionState> CSVMultiFileInfo::InitializeGlobalState(ClientContext &context,
                                                                             MultiFileBindData &bind_data,
                                                                             MultiFileGlobalState &global_state) {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();

	// Create the temporary rejects table
	if (csv_data.options.store_rejects.GetValue()) {
		CSVRejectsTable::GetOrCreate(context, csv_data.options.rejects_scan_name.GetValue(),
		                             csv_data.options.rejects_table_name.GetValue())
		    ->InitializeTable(context, csv_data);
	}
	return make_uniq<CSVGlobalState>(context, csv_data.options, bind_data.file_list->GetTotalFileCount(), bind_data);
}

struct CSVLocalState : public LocalTableFunctionState {
public:
	unique_ptr<StringValueScanner> csv_reader;
	bool done = false;
};

unique_ptr<LocalTableFunctionState> CSVMultiFileInfo::InitializeLocalState(ExecutionContext &,
                                                                           GlobalTableFunctionState &) {
	return make_uniq<CSVLocalState>();
}

shared_ptr<BaseFileReader> CSVMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                          BaseUnionData &union_data_p,
                                                          const MultiFileBindData &bind_data) {
	auto &union_data = union_data_p.Cast<CSVUnionData>();
	auto &gstate = gstate_p.Cast<CSVGlobalState>();
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	// union readers - use cached options
	auto &csv_names = union_data.names;
	auto &csv_types = union_data.types;
	auto options = union_data.options;
	options.auto_detect = false;
	D_ASSERT(csv_data.csv_schema.Empty());
	return make_shared_ptr<CSVFileScan>(context, union_data.GetFileName(), std::move(options), bind_data.file_options,
	                                    csv_names, csv_types, csv_data.csv_schema, gstate.SingleThreadedRead(), nullptr,
	                                    false);
}

shared_ptr<BaseFileReader> CSVMultiFileInfo::CreateReader(ClientContext &context, GlobalTableFunctionState &gstate_p,
                                                          const string &filename, idx_t file_idx,
                                                          const MultiFileBindData &bind_data) {
	auto &gstate = gstate_p.Cast<CSVGlobalState>();
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();

	auto options = csv_data.options;
	if (bind_data.file_list->GetExpandResult() == FileExpandResult::SINGLE_FILE) {
		options.auto_detect = false;
	}
	shared_ptr<CSVBufferManager> buffer_manager;
	if (file_idx == 0) {
		buffer_manager = csv_data.buffer_manager;
		if (buffer_manager && buffer_manager->GetFilePath() != filename) {
			buffer_manager.reset();
		}
	}
	return make_shared_ptr<CSVFileScan>(context, filename, std::move(options), bind_data.file_options, bind_data.names,
	                                    bind_data.types, csv_data.csv_schema, gstate.SingleThreadedRead(),
	                                    std::move(buffer_manager), false);
}

shared_ptr<BaseFileReader> CSVMultiFileInfo::CreateReader(ClientContext &context, const string &filename,
                                                          CSVReaderOptions &options,
                                                          const MultiFileReaderOptions &file_options) {
	return make_shared_ptr<CSVFileScan>(context, filename, options, file_options);
}

shared_ptr<BaseUnionData> CSVMultiFileInfo::GetUnionData(shared_ptr<BaseFileReader> scan_p, idx_t file_idx) {
	auto &scan = scan_p->Cast<CSVFileScan>();
	auto data = make_shared_ptr<CSVUnionData>(scan_p->GetFileName());
	if (file_idx == 0) {
		data->options = scan.options;
		data->names = scan.GetNames();
		data->types = scan.GetTypes();
		data->reader = std::move(scan_p);
	} else {
		data->options = std::move(scan.options);
		data->names = scan.GetNames();
		data->types = scan.GetTypes();
	}
	data->options.auto_detect = false;
	return data;
}

void CSVMultiFileInfo::FinalizeReader(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &) {
	auto &csv_file_scan = reader.Cast<CSVFileScan>();
	csv_file_scan.InitializeFileNamesTypes();
	csv_file_scan.SetStart();
}

bool CSVMultiFileInfo::TryInitializeScan(ClientContext &context, shared_ptr<BaseFileReader> &reader,
                                         GlobalTableFunctionState &gstate_p, LocalTableFunctionState &lstate_p) {
	auto &gstate = gstate_p.Cast<CSVGlobalState>();
	auto &lstate = lstate_p.Cast<CSVLocalState>();
	auto csv_reader_ptr = shared_ptr_cast<BaseFileReader, CSVFileScan>(reader);
	gstate.FinishScan(std::move(lstate.csv_reader));
	lstate.csv_reader = gstate.Next(csv_reader_ptr);
	if (!lstate.csv_reader) {
		// exhausted the scan
		return false;
	}
	return true;
}

void CSVMultiFileInfo::Scan(ClientContext &context, BaseFileReader &reader, GlobalTableFunctionState &global_state,
                            LocalTableFunctionState &local_state, DataChunk &chunk) {
	auto &lstate = local_state.Cast<CSVLocalState>();
	if (lstate.csv_reader->FinishedIterator()) {
		return;
	}
	lstate.csv_reader->Flush(chunk);
}

void CSVMultiFileInfo::FinishFile(ClientContext &context, GlobalTableFunctionState &global_state,
                                  BaseFileReader &reader) {
	auto &gstate = global_state.Cast<CSVGlobalState>();
	gstate.FinishLaunchingTasks(reader.Cast<CSVFileScan>());
}

void CSVMultiFileInfo::FinishReading(ClientContext &context, GlobalTableFunctionState &global_state,
                                     LocalTableFunctionState &local_state) {
	auto &gstate = global_state.Cast<CSVGlobalState>();
	auto &lstate = local_state.Cast<CSVLocalState>();
	gstate.FinishScan(std::move(lstate.csv_reader));
}

unique_ptr<NodeStatistics> CSVMultiFileInfo::GetCardinality(const MultiFileBindData &bind_data, idx_t file_count) {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	// determined through the scientific method as the average amount of rows in a CSV file
	idx_t per_file_cardinality = 42;
	if (csv_data.buffer_manager && csv_data.buffer_manager->file_handle) {
		auto estimated_row_width = (bind_data.types.size() * 5);
		per_file_cardinality = csv_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
	}
	return make_uniq<NodeStatistics>(file_count * per_file_cardinality);
}

unique_ptr<BaseStatistics> CSVMultiFileInfo::GetStatistics(ClientContext &context, BaseFileReader &reader,
                                                           const string &name) {
	throw InternalException("Unimplemented CSVMultiFileInfo method");
}

double CSVMultiFileInfo::GetProgressInFile(ClientContext &context, const BaseFileReader &reader) {
	auto &csv_scan = reader.Cast<CSVFileScan>();

	auto buffer_manager = csv_scan.buffer_manager;
	if (!buffer_manager) {
		// We are done with this file, so it's 100%
		return 100.0;
	}
	double bytes_read;
	if (buffer_manager->file_handle->compression_type == FileCompressionType::GZIP ||
	    buffer_manager->file_handle->compression_type == FileCompressionType::ZSTD) {
		// compressed file: we care about the progress made in the *underlying* file handle
		// the bytes read from the uncompressed file are skewed
		bytes_read = buffer_manager->file_handle->GetProgress();
	} else {
		bytes_read = static_cast<double>(csv_scan.bytes_read);
	}
	double file_progress = bytes_read / static_cast<double>(csv_scan.file_size);
	return file_progress * 100.0;
}

void CSVMultiFileInfo::GetVirtualColumns(ClientContext &, MultiFileBindData &, virtual_column_map_t &result) {
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
}

} // namespace duckdb
