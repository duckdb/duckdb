#include "duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp"

#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"

namespace duckdb {

CSVFileScan::CSVFileScan(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         const CSVReaderOptions &options_p)
    : file_path(options_p.file_path), file_idx(0), buffer_manager(buffer_manager_p), state_machine(state_machine_p),
      file_size(buffer_manager->file_handle->FileSize()),
      error_handler(make_shared<CSVErrorHandler>(options_p.ignore_errors)),
      on_disk_file(buffer_manager->file_handle->OnDiskFile()), options(options_p) {
}

CSVFileScan::CSVFileScan(ClientContext &context, const string &file_path_p, idx_t file_idx_p,
                         const CSVReaderOptions &options_p)
    : file_path(file_path_p), file_idx(file_idx_p),
      error_handler(make_shared<CSVErrorHandler>(options_p.ignore_errors)), options(options_p) {
	// Initialize Buffer Manager
	buffer_manager = make_shared<CSVBufferManager>(context, options, file_path, file_idx);
	// Initialize On Disk and Size of file
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	file_size = buffer_manager->file_handle->FileSize();
	// Sniff it (We only really care about dialect detection, if types or number of columns are different this will
	// error out during scanning)
	auto &state_machine_cache = *CSVStateMachineCache::Get(context);
	if (options.auto_detect && options.dialect_options.num_cols == 0) {
		CSVSniffer sniffer(options, buffer_manager, state_machine_cache);
		sniffer.SniffCSV();
	}
	if (options.dialect_options.num_cols == 0) {
		// We need to define the number of columns, if the sniffer is not running this must be in the sql_type_list
		options.dialect_options.num_cols = options.sql_type_list.size();
	}
	// Initialize State Machine
	state_machine =
	    make_shared<CSVStateMachine>(state_machine_cache.Get(options.dialect_options.state_machine_options), options);
}

// void SetupMultifileReader(){
//	if (!reader || reader->options.file_path != current_file_path) {
//		// we either don't have a reader, or the reader was created for a different file
//		// we need to create a new reader and instantiate it
//		if (file_index > 0 && file_index <= bind_data.union_readers.size() && bind_data.union_readers[file_index - 1]) {
//			// we are doing UNION BY NAME - fetch the options from the union reader for this file
//			auto &union_reader = *bind_data.union_readers[file_index - 1];
//			reader = make_uniq<ParallelCSVReader>(context, union_reader.options, std::move(result), first_position,
//			                                      union_reader.GetTypes(), file_index - 1);
//			reader->names = union_reader.GetNames();
//		} else if (file_index <= bind_data.column_info.size()) {
//			// Serialized Union By name
//			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result), first_position,
//			                                      bind_data.column_info[file_index - 1].types, file_index - 1);
//			reader->names = bind_data.column_info[file_index - 1].names;
//		} else {
//			// regular file - use the standard options
//			if (!result) {
//				return false;
//			}
//			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result), first_position,
//			                                      bind_data.csv_types, file_index - 1);
//			reader->names = bind_data.csv_names;
//		}
//		reader->options.file_path = current_file_path;
//		MultiFileReader::InitializeReader(*reader, bind_data.options.file_options, bind_data.reader_bind,
//		                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
//		                                  bind_data.files.front(), context);
//	}
//}

CSVGlobalState::CSVGlobalState(ClientContext &context_p, shared_ptr<CSVBufferManager> buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<column_t> column_ids_p)
    : context(context_p), system_threads(system_threads_p), column_ids(std::move(column_ids_p)),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error) {

	if (buffer_manager) {
		auto state_machine = make_shared<CSVStateMachine>(
		    CSVStateMachineCache::Get(context)->Get(options.dialect_options.state_machine_options), options);
		// If we already have a buffer manager, we don't need to reconstruct it to the first file
		file_scans.emplace_back(make_uniq<CSVFileScan>(buffer_manager, state_machine, options));
	} else {
		// If not we need to construct it for the first file
		file_scans.emplace_back(make_uniq<CSVFileScan>(context, files[0], 0, options));
	};

	//! There are situations where we only support single threaded scanning
	bool single_threaded = options.null_padding;

	if (!single_threaded) {
		running_threads = MaxThreads();
	}
	current_boundary = CSVIterator(0, 0, 0, 0);
}

double CSVGlobalState::GetProgress(const ReadCSVData &bind_data) const {

	idx_t total_files = bind_data.files.size();
	// get the progress WITHIN the current file
	double progress;
	if (file_scans.back()->file_size == 0) {
		progress = 1.0;
	} else {
		// for compressed files, readed bytes may greater than files size.
		progress = std::min(1.0, double(file_scans.back()->bytes_read) / double(file_scans.back()->file_size));
	}
	// now get the total percentage of files read
	double percentage = double(current_boundary.GetFileIdx()) / total_files;
	percentage += (double(1) / double(total_files)) * progress;
	return percentage * 100;
}

unique_ptr<StringValueScanner> CSVGlobalState::InitializeScanner(const ReadCSVData &bind_data) {
	auto &current_file = *file_scans.back();
	auto csv_scanner = make_uniq<StringValueScanner>(current_file.buffer_manager, current_file.state_machine,
	                                                 current_file.error_handler, current_boundary);
	csv_scanner->file_path = bind_data.files.front();
	csv_scanner->names = bind_data.return_names;
	csv_scanner->types = bind_data.return_types;
	MultiFileReader::InitializeReader(*csv_scanner, bind_data.options.file_options, bind_data.reader_bind,
	                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
	                                  bind_data.files.front(), context);

	return csv_scanner;
}
unique_ptr<StringValueScanner> CSVGlobalState::Next(const ReadCSVData &bind_data) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (finished) {
		return nullptr;
	}
	auto next_scanner = InitializeScanner(bind_data);
	// We first check if we are done with this file
	auto &current_file = *file_scans.back();
	if (!current_boundary.Next(*current_file.buffer_manager)) {
		// This means we are done scanning the current file
		auto current_file_idx = current_file.file_idx + 1;
		if (current_file_idx < bind_data.files.size()) {
			// If we have a next file we have to construct the file scan for that
			file_scans.emplace_back(make_uniq<CSVFileScan>(context, bind_data.files[current_file_idx], current_file_idx,
			                                               bind_data.options));
			// And re-start the boundary-iterator
			current_boundary = CSVIterator(current_file_idx, 0, 0, current_boundary.GetBoundaryIdx() + 1);
		} else {
			// If not we are done with this CSV Scanning
			finished = true;
		}
	}
	// We initialize the scan
	return next_scanner;
}

idx_t CSVGlobalState::MaxThreads() const {
	// We initialize max one thread per our set bytes per thread limit
	idx_t total_threads = file_scans.back()->file_size / CSVIterator::BYTES_PER_THREAD + 1;
	if (total_threads < system_threads) {
		return total_threads;
	}
	return system_threads;
}

void CSVGlobalState::DecrementThread() {
	lock_guard<mutex> parallel_lock(main_mutex);
	D_ASSERT(running_threads > 0);
	running_threads--;
}

bool CSVGlobalState::Finished() {
	lock_guard<mutex> parallel_lock(main_mutex);
	return running_threads == 0;
}

} // namespace duckdb
