#include "duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp"

#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context_p, shared_ptr<CSVBufferManager> buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<column_t> column_ids_p, const ReadCSVData &bind_data_p)
    : context(context_p), system_threads(system_threads_p), column_ids(std::move(column_ids_p)),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error), bind_data(bind_data_p) {

	if (buffer_manager && buffer_manager->GetFilePath() == files[0]) {
		auto state_machine = make_shared<CSVStateMachine>(
		    CSVStateMachineCache::Get(context)->Get(options.dialect_options.state_machine_options), options);
		// If we already have a buffer manager, we don't need to reconstruct it to the first file
		file_scans.emplace_back(make_uniq<CSVFileScan>(context, buffer_manager, state_machine, options, bind_data,
		                                               column_ids, file_schema));
	} else {
		// If not we need to construct it for the first file
		file_scans.emplace_back(
		    make_uniq<CSVFileScan>(context, files[0], options, 0, bind_data, column_ids, file_schema));
	};

	//! There are situations where we only support single threaded scanning
	bool many_csv_files = files.size() > 1 && files.size() * 2 >= system_threads;
	single_threaded = options.null_padding || many_csv_files;

	if (!single_threaded) {
		running_threads = MaxThreads();
	}
	current_boundary = CSVIterator(0, 0, 0, 0);
}

double CSVGlobalState::GetProgress(const ReadCSVData &bind_data_p) const {

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

unique_ptr<StringValueScanner> CSVGlobalState::Next() {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (finished) {
		return nullptr;
	}
	// We first create the scanner for the currenot boundary
	auto &current_file = *file_scans.back();
	if (single_threaded){
		current_boundary = CSVIterator();
	}
	auto csv_scanner = make_uniq<StringValueScanner>(current_file.buffer_manager, current_file.state_machine,
	                                                 current_file.error_handler, current_boundary);
	csv_scanner->csv_file_scan = file_scans.back();
	// We then produce the next boundary
	if (!current_boundary.Next(*current_file.buffer_manager)) {
		// This means we are done scanning the current file
		auto current_file_idx = current_file.file_idx + 1;
		if (current_file_idx < bind_data.files.size()) {
			// If we have a next file we have to construct the file scan for that
			file_scans.emplace_back(make_shared<CSVFileScan>(context, bind_data.files[current_file_idx],
			                                                 bind_data.options, current_file_idx, bind_data, column_ids,
			                                                 file_schema));
			// And re-start the boundary-iterator
			current_boundary = CSVIterator(current_file_idx, 0, 0, current_boundary.GetBoundaryIdx() + 1);
		} else {
			// If not we are done with this CSV Scanning
			finished = true;
		}
	}
	// We initialize the scan
	return csv_scanner;
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
