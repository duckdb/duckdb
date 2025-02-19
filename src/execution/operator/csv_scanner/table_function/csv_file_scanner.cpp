#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_path_p, CSVReaderOptions options_p,
                         idx_t file_idx_p, const vector<string> &names, const vector<LogicalType> &types,
                         const vector<ColumnIndex> &column_ids, CSVSchema &file_schema, bool per_file_single_threaded,
                         shared_ptr<CSVBufferManager> buffer_manager_p, bool fixed_schema)
    : BaseFileReader(file_path_p), file_idx(file_idx_p), buffer_manager(std::move(buffer_manager_p)),
      error_handler(make_shared_ptr<CSVErrorHandler>(options_p.ignore_errors.GetValue())),
      options(std::move(options_p)) {

	// Initialize Buffer Manager
	if (!buffer_manager) {
		buffer_manager =
		    make_shared_ptr<CSVBufferManager>(context, options, file_name, file_idx, per_file_single_threaded);
	}
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Initialize State Machine
	auto &state_machine_cache = CSVStateMachineCache::Get(context);

	SetNamesAndTypes(names, types);
	if (options.auto_detect) {
		if (fixed_schema) {
			// schema of the file is fixed - only run the sniffer
			CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
			sniffer.SniffCSV();
		} else if (file_schema.Empty()) {
			// no schema yet - run the sniffer and initialize the schema
			CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
			auto result = sniffer.SniffCSV();
			file_schema.Initialize(names, types, options.file_path);
		} else if (file_idx > 0 && buffer_manager->file_handle->FileSize() > 0) {
			options.file_path = file_name;
			CSVSniffer sniffer(options, buffer_manager, state_machine_cache, false);
			auto result = sniffer.AdaptiveSniff(file_schema);
			SetNamesAndTypes(result.names, result.return_types);
		}
	}
	if (options.dialect_options.num_cols == 0) {
		// We need to define the number of columns, if the sniffer is not running this must be in the sql_type_list
		options.dialect_options.num_cols = options.sql_type_list.size();
	}
	if (options.dialect_options.state_machine_options.new_line == NewLineIdentifier::NOT_SET) {
		options.dialect_options.state_machine_options.new_line = CSVSniffer::DetectNewLineDelimiter(*buffer_manager);
	}
	state_machine = make_shared_ptr<CSVStateMachine>(
	    state_machine_cache.Get(options.dialect_options.state_machine_options), options);
}

CSVUnionData::~CSVUnionData() {
}

void CSVFileScan::SetStart() {
	idx_t rows_to_skip = options.GetSkipRows() + state_machine->dialect_options.header.GetValue();
	rows_to_skip = std::max(rows_to_skip, state_machine->dialect_options.rows_until_header +
	                                          state_machine->dialect_options.header.GetValue());
	if (rows_to_skip == 0) {
		start_iterator.first_one = true;
		return;
	}
	SkipScanner skip_scanner(buffer_manager, state_machine, error_handler, rows_to_skip);
	skip_scanner.ParseChunk();
	start_iterator = skip_scanner.GetIterator();
}

void CSVFileScan::SetNamesAndTypes(const vector<string> &names_p, const vector<LogicalType> &types_p) {
	names = names_p;
	types = types_p;
	columns = MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(names, types);
}

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_name, const CSVReaderOptions &options_p)
    : BaseFileReader(file_name), file_idx(0),
      error_handler(make_shared_ptr<CSVErrorHandler>(options_p.ignore_errors.GetValue())), options(options_p) {
	buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, file_name, file_idx);
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Sniff it (We only really care about dialect detection, if types or number of columns are different this will
	// error out during scanning)
	auto &state_machine_cache = CSVStateMachineCache::Get(context);
	// We sniff file if it has not been sniffed yet and either auto-detect is on, or union by name is on
	if ((options.auto_detect || options.file_options.union_by_name) && options.dialect_options.num_cols == 0) {
		CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
		auto sniffer_result = sniffer.SniffCSV();
		if (names.empty()) {
			SetNamesAndTypes(sniffer_result.names, sniffer_result.return_types);
		}
	}
	if (options.dialect_options.num_cols == 0) {
		// We need to define the number of columns, if the sniffer is not running this must be in the sql_type_list
		options.dialect_options.num_cols = options.sql_type_list.size();
	}
	// Initialize State Machine
	state_machine = make_shared_ptr<CSVStateMachine>(
	    state_machine_cache.Get(options.dialect_options.state_machine_options), options);
	SetStart();
}

void CSVFileScan::InitializeFileNamesTypes() {
	if (reader_data.empty_columns && reader_data.column_ids.empty()) {
		// This means that the columns from this file are irrelevant.
		// just read the first column
		file_types.emplace_back(LogicalType::VARCHAR);
		projected_columns.insert(0);
		projection_ids.emplace_back(0, 0);
		return;
	}

	for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
		idx_t result_idx = reader_data.column_ids[i];
		file_types.emplace_back(types[result_idx]);
		projected_columns.insert(result_idx);
		projection_ids.emplace_back(result_idx, i);
	}

	if (reader_data.column_ids.empty()) {
		file_types = types;
	}

	// We need to be sure that our types are also following the cast_map
	if (!reader_data.cast_map.empty()) {
		for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
			if (reader_data.cast_map.find(reader_data.column_ids[i]) != reader_data.cast_map.end()) {
				file_types[i] = reader_data.cast_map[reader_data.column_ids[i]];
			}
		}
	}

	// We sort the types on the order of the parsed chunk
	std::sort(projection_ids.begin(), projection_ids.end());
	vector<LogicalType> sorted_types;
	for (idx_t i = 0; i < projection_ids.size(); ++i) {
		sorted_types.push_back(file_types[projection_ids[i].second]);
	}
	file_types = sorted_types;
}

const vector<string> &CSVFileScan::GetNames() {
	return names;
}
const vector<LogicalType> &CSVFileScan::GetTypes() {
	return types;
}

void CSVFileScan::InitializeProjection() {
	for (idx_t i = 0; i < options.dialect_options.num_cols; i++) {
		reader_data.column_ids.push_back(i);
		reader_data.column_mapping.push_back(i);
	}
}

void CSVFileScan::Finish() {
	buffer_manager.reset();
}

} // namespace duckdb
