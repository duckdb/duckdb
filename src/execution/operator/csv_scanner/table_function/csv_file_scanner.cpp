#include "duckdb/execution/operator/csv_scanner/csv_file_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {
CSVUnionData::~CSVUnionData() {
}

CSVFileScan::CSVFileScan(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager_p,
                         shared_ptr<CSVStateMachine> state_machine_p, const CSVReaderOptions &options_p,
                         const ReadCSVData &bind_data, const vector<ColumnIndex> &column_ids, CSVSchema &file_schema)
    : file_path(options_p.file_path), file_idx(0), buffer_manager(std::move(buffer_manager_p)),
      state_machine(std::move(state_machine_p)), file_size(buffer_manager->file_handle->FileSize()),
      error_handler(make_shared_ptr<CSVErrorHandler>(options_p.ignore_errors.GetValue())),
      on_disk_file(buffer_manager->file_handle->OnDiskFile()), options(options_p) {

	auto multi_file_reader = MultiFileReader::CreateDefault("CSV Scan");
	if (bind_data.initial_reader.get()) {
		auto &union_reader = *bind_data.initial_reader;
		names = union_reader.GetNames();
		options = union_reader.options;
		types = union_reader.GetTypes();
		multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);
		InitializeFileNamesTypes();
		return;
	}
	if (!bind_data.column_info.empty()) {
		// Serialized Union By name
		names = bind_data.column_info[0].names;
		types = bind_data.column_info[0].types;
		multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);
		InitializeFileNamesTypes();
		return;
	}
	names = bind_data.csv_names;
	types = bind_data.csv_types;
	file_schema.Initialize(names, types, file_path);
	multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
	                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);

	InitializeFileNamesTypes();
	SetStart();
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

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_path_p, const CSVReaderOptions &options_p,
                         const idx_t file_idx_p, const ReadCSVData &bind_data, const vector<ColumnIndex> &column_ids,
                         CSVSchema &file_schema, bool per_file_single_threaded)
    : file_path(file_path_p), file_idx(file_idx_p),
      error_handler(make_shared_ptr<CSVErrorHandler>(options_p.ignore_errors.GetValue())), options(options_p) {
	auto multi_file_reader = MultiFileReader::CreateDefault("CSV Scan");
	if (file_idx == 0 && bind_data.initial_reader) {
		auto &union_reader = *bind_data.initial_reader;
		// Initialize Buffer Manager
		buffer_manager = union_reader.buffer_manager;
		// Initialize On Disk and Size of file
		on_disk_file = union_reader.on_disk_file;
		file_size = union_reader.file_size;
		names = union_reader.GetNames();
		options = union_reader.options;
		types = union_reader.GetTypes();
		state_machine = union_reader.state_machine;
		multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);

		InitializeFileNamesTypes();
		SetStart();
		return;
	}

	// Initialize Buffer Manager
	buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, file_path, file_idx, per_file_single_threaded);
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Initialize State Machine
	auto &state_machine_cache = CSVStateMachineCache::Get(context);

	if (file_idx < bind_data.column_info.size()) {
		// (Serialized) Union By name
		names = bind_data.column_info[file_idx].names;
		types = bind_data.column_info[file_idx].types;
		if (file_idx < bind_data.union_readers.size()) {
			// union readers - use cached options
			D_ASSERT(names == bind_data.union_readers[file_idx]->names);
			D_ASSERT(types == bind_data.union_readers[file_idx]->types);
			options = bind_data.union_readers[file_idx]->options;
		} else {
			// Serialized union by name - sniff again
			options.dialect_options.num_cols = names.size();
			if (options.auto_detect) {
				CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
				sniffer.SniffCSV();
			}
		}
		state_machine = make_shared_ptr<CSVStateMachine>(
		    state_machine_cache.Get(options.dialect_options.state_machine_options), options);

		multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);
		InitializeFileNamesTypes();
		SetStart();
		return;
	}
	// Sniff it!
	names = bind_data.csv_names;
	types = bind_data.csv_types;
	if (options.auto_detect && bind_data.files.size() > 1) {
		if (file_schema.Empty()) {
			CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
			auto result = sniffer.SniffCSV();
			file_schema.Initialize(result.names, result.return_types, options.file_path);
		} else if (file_idx > 0 && buffer_manager->file_handle->FileSize() > 0) {
			options.file_path = file_path;
			CSVSniffer sniffer(options, buffer_manager, state_machine_cache, false);
			auto result = sniffer.AdaptiveSniff(file_schema);
			names = result.names;
			types = result.return_types;
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
	multi_file_reader->InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
	                                    bind_data.return_names, column_ids, nullptr, file_path, context, nullptr);
	InitializeFileNamesTypes();
	SetStart();
}

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_name, const CSVReaderOptions &options_p)
    : file_path(file_name), file_idx(0),
      error_handler(make_shared_ptr<CSVErrorHandler>(options_p.ignore_errors.GetValue())), options(options_p) {
	buffer_manager = make_shared_ptr<CSVBufferManager>(context, options, file_path, file_idx);
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Sniff it (We only really care about dialect detection, if types or number of columns are different this will
	// error out during scanning)
	auto &state_machine_cache = CSVStateMachineCache::Get(context);
	if (options.auto_detect && options.dialect_options.num_cols == 0) {
		CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
		auto sniffer_result = sniffer.SniffCSV();
		if (names.empty()) {
			names = sniffer_result.names;
			types = sniffer_result.return_types;
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

const string &CSVFileScan::GetFileName() const {
	return file_path;
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
