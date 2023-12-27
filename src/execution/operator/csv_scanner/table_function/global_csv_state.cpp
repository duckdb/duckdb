#include "duckdb/execution/operator/scan/csv/table_function/global_csv_state.hpp"
#include "duckdb/execution/operator/scan/csv/scanner/scanner_boundary.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context, shared_ptr<CSVBufferManager> buffer_manager_p,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<column_t> column_ids_p, const StateMachine &state_machine_p)
    : buffer_manager(std::move(buffer_manager_p)), system_threads(system_threads_p),
      column_ids(std::move(column_ids_p)),
      line_info(main_mutex, batch_to_tuple_end, tuple_start, tuple_end, options.sniffer_user_mismatch_error),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error) {

	state_machine = make_shared<CSVStateMachine>(cache.Get(options.dialect_options.state_machine_options), options);
	//! If the buffer manager has not yet being initialized, we do it now.
	if (!buffer_manager) {
		buffer_manager = make_shared<CSVBufferManager>(context, options, files);
	}
	//! There are situations where we only support single threaded scanning
	bool single_threaded = options.null_padding;

	//! Set information regarding file_size, if it's on disk and use that to set number of threads that will
	//! be used in this scanner
	file_size = buffer_manager->file_handle->FileSize();
	on_disk_file = buffer_manager->file_handle->OnDiskFile();
	if (!single_threaded) {
		running_threads = MaxThreads();
	}

	//! Initialize all the book-keeping variables used in verification
	InitializeVerificationVariables(options, files.size());

	//! Initialize the lines read
	line_info.lines_read[0][0] = options.dialect_options.skip_rows.GetValue();
	if (options.dialect_options.header.GetValue()) {
		line_info.lines_read[0][0]++;
	}
}

void CSVGlobalState::InitializeVerificationVariables(const CSVReaderOptions &options, idx_t file_count) {
	line_info.current_batches.resize(file_count);
	line_info.lines_read.resize(file_count);
	line_info.lines_errored.resize(file_count);
	tuple_start.resize(file_count);
	tuple_end.resize(file_count);
	tuple_end_to_batch.resize(file_count);
	batch_to_tuple_end.resize(file_count);

	// Initialize the lines read
	line_info.lines_read[0][0] = options.dialect_options.skip_rows.GetValue();
	if (options.dialect_options.header.GetValue()) {
		line_info.lines_read[0][0]++;
	}
}

double CSVGlobalState::GetProgress(const ReadCSVData &bind_data) const {

	idx_t total_files = bind_data.files.size();
	// get the progress WITHIN the current file
	double progress;
	if (file_size == 0) {
		progress = 1.0;
	} else {
		progress = double(bytes_read) / double(file_size);
	}
	// now get the total percentage of files read
	double percentage = double(current_boundary.GetFileIdx()) / total_files;
	percentage += (double(1) / double(total_files)) * progress;
	return percentage * 100;
}

unique_ptr<StringValueScanner> CSVGlobalState::Next(ClientContext &context, const ReadCSVData &bind_data) {
	lock_guard<mutex> parallel_lock(main_mutex);
	if (finished) {
		return nullptr;
	}
	auto csv_scanner = make_uniq<StringValueScanner>(buffer_manager, state_machine, current_boundary);
	finished = current_boundary.Next(*buffer_manager);

	csv_scanner->file_path = bind_data.files.front();
	csv_scanner->names = bind_data.return_names;
	csv_scanner->types = bind_data.return_types;
	MultiFileReader::InitializeReader(*csv_scanner, bind_data.options.file_options, bind_data.reader_bind,
	                                  bind_data.return_types, bind_data.return_names, column_ids, nullptr,
	                                  bind_data.files.front(), context);

	return csv_scanner;

	//		if (!reader) {
	//			D_ASSERT(0);
	//			//		// we either don't have a reader, or the reader was created for a different file
	//			//		// we need to create a new reader and instantiate it
	//			//		if (file_index > 0 && file_index <= bind_data.union_readers.size() &&
	//	 bind_data.union_readers[file_index
	//			//- 1]) {
	//			//			// we are doing UNION BY NAME - fetch the options from the union reader for this file
	//			//			auto &union_reader = *bind_data.union_readers[file_index - 1];
	//			//			reader = make_uniq<ParallelCSVReader>(context, union_reader.options, std::move(result),
	//			// first_position, 			                                      union_reader.GetTypes(), file_index -
	// 1);
	//			// reader->names = union_reader.GetNames(); 		} else if (file_index <=
	// bind_data.column_info.size())
	//{
	//			//			// Serialized Union By name
	//			//			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result),
	//	 first_position,
	//			//			                                      bind_data.column_info[file_index - 1].types,
	// file_index
	//- 	 1);
	//			//			reader->names = bind_data.column_info[file_index - 1].names;
	//			//		} else {
	//			//			// regular file - use the standard options
	//			//			if (!result) {
	//			//				return false;
	//			//			}
	//			//			reader = make_uniq<ParallelCSVReader>(context, bind_data.options, std::move(result),
	//	 first_position,
	//			//			                                      bind_data.csv_types, file_index - 1);
	//			//			reader->names = bind_data.csv_names;
	//			//		}
	//			//		reader->options.file_path = current_file_path;
	//			//		MultiFileReader::InitializeReader(*reader, bind_data.options.file_options,
	// bind_data.reader_bind,
	//			//		                                  bind_data.return_types, bind_data.return_names, column_ids,
	//	 nullptr,
	//			//		                                  bind_data.files.front(), context);
	//		} else {
	//			// update the current reader
	//			//		reader->SetBufferRead(std::move(result));
	//		}
	//
	//		return true;
}

// void CSVGlobalState::UpdateVerification(VerificationPositions positions, idx_t file_number_p, idx_t batch_idx) {
//	lock_guard<mutex> parallel_lock(main_mutex);
//	if (positions.end_of_last_line > max_tuple_end) {
//		max_tuple_end = positions.end_of_last_line;
//	}
//	tuple_end_to_batch[file_number_p][positions.end_of_last_line] = batch_idx;
//	batch_to_tuple_end[file_number_p][batch_idx] = tuple_end[file_number_p].size();
//	tuple_start[file_number_p].insert(positions.beginning_of_first_line);
//	tuple_end[file_number_p].push_back(positions.end_of_last_line);
//}
//
// void CSVGlobalState::UpdateLinesRead(CSVScanner &buffer_read, idx_t file_idx) {
//	auto batch_idx = buffer_read.scanner_id;
//	auto lines_read = buffer_read.GetTotalRowsEmmited();
//	lock_guard<mutex> parallel_lock(main_mutex);
//	line_info.current_batches[file_idx].erase(batch_idx);
//	line_info.lines_read[file_idx][batch_idx] += lines_read;
//}

idx_t CSVGlobalState::MaxThreads() const {
	// We initialize max one thread per our set bytes per thread limit
	idx_t total_threads = file_size / ScannerBoundary::BYTES_PER_THREAD + 1;
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

void CSVGlobalState::Verify() {
	// All threads are done, we run some magic sweet verification code
	lock_guard<mutex> parallel_lock(main_mutex);
	if (running_threads == 0) {
		D_ASSERT(tuple_end.size() == tuple_start.size());
		for (idx_t i = 0; i < tuple_start.size(); i++) {
			auto &current_tuple_end = tuple_end[i];
			auto &current_tuple_start = tuple_start[i];
			// figure out max value of last_pos
			if (current_tuple_end.empty()) {
				return;
			}
			auto max_value = *max_element(std::begin(current_tuple_end), std::end(current_tuple_end));
			for (idx_t tpl_idx = 0; tpl_idx < current_tuple_end.size(); tpl_idx++) {
				auto last_pos = current_tuple_end[tpl_idx];
				auto first_pos = current_tuple_start.find(last_pos);
				if (first_pos == current_tuple_start.end()) {
					// this might be necessary due to carriage returns outside buffer scopes.
					first_pos = current_tuple_start.find(last_pos + 1);
				}
				if (first_pos == current_tuple_start.end() && last_pos != max_value) {
					auto batch_idx = tuple_end_to_batch[i][last_pos];
					auto problematic_line = line_info.GetLine(batch_idx);
					throw InvalidInputException(
					    "CSV File not supported for multithreading. This can be a problematic line in your CSV File or "
					    "that this CSV can't be read in Parallel. Please, inspect if the line %llu is correct. If so, "
					    "please run single-threaded CSV Reading by setting parallel=false in the read_csv call. %s",
					    problematic_line, sniffer_mismatch_error);
				}
			}
		}
	}
}

} // namespace duckdb
