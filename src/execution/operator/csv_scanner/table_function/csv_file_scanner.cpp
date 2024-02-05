#include "duckdb/execution/operator/csv_scanner/table_function/csv_file_scanner.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"

namespace duckdb {

CSVFileScan::CSVFileScan(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager_p,
                         shared_ptr<CSVStateMachine> state_machine_p, const CSVReaderOptions &options_p,
                         const ReadCSVData &bind_data, const vector<column_t> &column_ids,
                         vector<LogicalType> &file_schema)
    : file_path(options_p.file_path), file_idx(0), buffer_manager(std::move(buffer_manager_p)),
      state_machine(std::move(state_machine_p)), file_size(buffer_manager->file_handle->FileSize()),
      error_handler(make_shared<CSVErrorHandler>(options_p.ignore_errors)),
      on_disk_file(buffer_manager->file_handle->OnDiskFile()), options(options_p) {
	if (bind_data.initial_reader.get()) {
		auto &union_reader = *bind_data.initial_reader;
		names = union_reader.GetNames();
		options = union_reader.options;
		types = union_reader.GetTypes();
		MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                  bind_data.return_names, column_ids, nullptr, file_path, context);
		InitializeFileNamesTypes();
		return;
	} else if (!bind_data.column_info.empty()) {
		// Serialized Union By name
		names = bind_data.column_info[0].names;
		types = bind_data.column_info[0].types;
		MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                  bind_data.return_names, column_ids, nullptr, file_path, context);
		InitializeFileNamesTypes();
		return;
	}
	names = bind_data.return_names;
	types = bind_data.return_types;
	file_schema = bind_data.return_types;
	MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
	                                  bind_data.return_names, column_ids, nullptr, file_path, context);

	InitializeFileNamesTypes();
}

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_path_p, const CSVReaderOptions &options_p,
                         const idx_t file_idx_p, const ReadCSVData &bind_data, const vector<column_t> &column_ids,
                         const vector<LogicalType> &file_schema)
    : file_path(file_path_p), file_idx(file_idx_p),
      error_handler(make_shared<CSVErrorHandler>(options_p.ignore_errors)), options(options_p) {
	if (file_idx < bind_data.union_readers.size()) {
		// we are doing UNION BY NAME - fetch the options from the union reader for this file
		optional_ptr<CSVFileScan> union_reader_ptr;
		if (file_idx == 0) {
			union_reader_ptr = bind_data.initial_reader.get();
		} else {
			union_reader_ptr = bind_data.union_readers[file_idx].get();
		}
		if (union_reader_ptr) {
			auto &union_reader = *union_reader_ptr;
			// Initialize Buffer Manager
			buffer_manager = union_reader.buffer_manager;
			// Initialize On Disk and Size of file
			on_disk_file = union_reader.on_disk_file;
			file_size = union_reader.file_size;
			names = union_reader.GetNames();
			options = union_reader.options;
			types = union_reader.GetTypes();
			state_machine = union_reader.state_machine;
			MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind,
			                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
			                                  file_path, context);

			InitializeFileNamesTypes();
			return;
		}
	}

	// Initialize Buffer Manager
	buffer_manager = make_shared<CSVBufferManager>(context, options, file_path, file_idx);
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Initialize State Machine
	auto &state_machine_cache = CSVStateMachineCache::Get(context);

	if (file_idx < bind_data.column_info.size()) {
		// Serialized Union By name
		names = bind_data.column_info[file_idx].names;
		types = bind_data.column_info[file_idx].types;
		options.dialect_options.num_cols = names.size();
		state_machine = make_shared<CSVStateMachine>(
		    state_machine_cache.Get(options.dialect_options.state_machine_options), options);

		MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
		                                  bind_data.return_names, column_ids, nullptr, file_path, context);
		InitializeFileNamesTypes();
		return;
	}
	// Sniff it (We only really care about dialect detection, if types or number of columns are different this will
	// error out during scanning)
	if (options.auto_detect && file_idx > 0) {
		CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
		auto result = sniffer.SniffCSV();
		if (!file_schema.empty()) {
			if (!options.file_options.filename && !options.file_options.hive_partitioning &&
			    file_schema.size() != result.return_types.size()) {
				throw InvalidInputException("Mismatch between the schema of different files");
			}
		}
	}
	if (options.dialect_options.num_cols == 0) {
		// We need to define the number of columns, if the sniffer is not running this must be in the sql_type_list
		options.dialect_options.num_cols = options.sql_type_list.size();
	}

	if (options.dialect_options.state_machine_options.new_line == NewLineIdentifier::NOT_SET) {
		options.dialect_options.state_machine_options.new_line = CSVSniffer::DetectNewLineDelimiter(*buffer_manager);
	}

	names = bind_data.return_names;
	types = bind_data.return_types;
	state_machine =
	    make_shared<CSVStateMachine>(state_machine_cache.Get(options.dialect_options.state_machine_options), options);

	MultiFileReader::InitializeReader(*this, options.file_options, bind_data.reader_bind, bind_data.return_types,
	                                  bind_data.return_names, column_ids, nullptr, file_path, context);
	InitializeFileNamesTypes();
}

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_name, CSVReaderOptions &options_p)
    : file_path(file_name), file_idx(0), error_handler(make_shared<CSVErrorHandler>(options_p.ignore_errors)),
      options(options_p) {
	buffer_manager = make_shared<CSVBufferManager>(context, options, file_path, file_idx);
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
	state_machine =
	    make_shared<CSVStateMachine>(state_machine_cache.Get(options.dialect_options.state_machine_options), options);
}

void CSVFileScan::InitializeFileNamesTypes() {
	if (options.null_padding && reader_data.column_ids.empty()) {
		// If we are null padding we do not yet support projection pushdown
		file_types = types;
		return;
	}
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

	if (!projected_columns.empty()) {
		// We might have to add recovery rejects column ids
		for (idx_t i = 0; i < options.rejects_recovery_column_ids.size(); i++) {
			idx_t col_id = options.rejects_recovery_column_ids[i];
			if (projected_columns.find(col_id) == projected_columns.end()) {
				// We have to insert this column in our projection
				projected_columns.insert(col_id);
				file_types.emplace_back(LogicalType::VARCHAR);
				projected_columns.insert(col_id);
				projection_ids.emplace_back(col_id, col_id);
			}
		}
	}

	if (reader_data.column_ids.empty()) {
		file_types = types;
	}

	// We need to be sure that our types are also following the cast_map
	for (idx_t i = 0; i < reader_data.column_ids.size(); i++) {
		if (reader_data.cast_map.find(reader_data.column_ids[i]) != reader_data.cast_map.end()) {
			file_types[i] = reader_data.cast_map[reader_data.column_ids[i]];
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

const string &CSVFileScan::GetFileName() {
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
} // namespace duckdb
