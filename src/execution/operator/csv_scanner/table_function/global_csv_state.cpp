#include "duckdb/execution/operator/csv_scanner/table_function/global_csv_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/appender.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<column_t> column_ids_p, const ReadCSVData &bind_data_p)
    : context(context_p), system_threads(system_threads_p), column_ids(std::move(column_ids_p)),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error), bind_data(bind_data_p) {

	if (buffer_manager && buffer_manager->GetFilePath() == files[0]) {
		auto state_machine = make_shared<CSVStateMachine>(
		    CSVStateMachineCache::Get(context).Get(options.dialect_options.state_machine_options), options);
		// If we already have a buffer manager, we don't need to reconstruct it to the first file
		file_scans.emplace_back(make_uniq<CSVFileScan>(context, buffer_manager, state_machine, options, bind_data,
		                                               column_ids, file_schema));
	} else {
		// If not we need to construct it for the first file
		file_scans.emplace_back(
		    make_uniq<CSVFileScan>(context, files[0], options, 0, bind_data, column_ids, file_schema));
	};
	//! There are situations where we only support single threaded scanning
	bool many_csv_files = files.size() > 1 && files.size() > system_threads * 2;
	single_threaded = many_csv_files || !options.parallel;
	last_file_idx = 0;
	scanner_idx = 0;
	running_threads = MaxThreads();
	if (single_threaded) {
		current_boundary = CSVIterator();
	} else {
		auto buffer_size = file_scans.back()->buffer_manager->GetBuffer(0)->actual_size;
		current_boundary = CSVIterator(0, 0, 0, 0, buffer_size);
	}
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
	if (single_threaded) {
		idx_t cur_idx = last_file_idx++;
		if (cur_idx >= bind_data.files.size()) {
			return nullptr;
		}
		shared_ptr<CSVFileScan> current_file;
		if (cur_idx == 0) {
			current_file = file_scans.back();
		} else {
			current_file = make_shared<CSVFileScan>(context, bind_data.files[cur_idx], bind_data.options, cur_idx,
			                                        bind_data, column_ids, file_schema);
		}
		auto csv_scanner =
		    make_uniq<StringValueScanner>(scanner_idx++, current_file->buffer_manager, current_file->state_machine,
		                                  current_file->error_handler, current_file, current_boundary);
		return csv_scanner;
	}
	lock_guard<mutex> parallel_lock(main_mutex);
	if (finished) {
		return nullptr;
	}

	// We first create the scanner for the current boundary
	auto &current_file = *file_scans.back();
	auto csv_scanner =
	    make_uniq<StringValueScanner>(scanner_idx++, current_file.buffer_manager, current_file.state_machine,
	                                  current_file.error_handler, file_scans.back(), current_boundary);
	// We then produce the next boundary
	if (!current_boundary.Next(*current_file.buffer_manager)) {
		// This means we are done scanning the current file
		auto current_file_idx = current_file.file_idx + 1;
		if (current_file_idx < bind_data.files.size()) {
			file_scans.back()->buffer_manager.reset();
			// If we have a next file we have to construct the file scan for that
			file_scans.emplace_back(make_shared<CSVFileScan>(context, bind_data.files[current_file_idx],
			                                                 bind_data.options, current_file_idx, bind_data, column_ids,
			                                                 file_schema));
			// And re-start the boundary-iterator
			auto buffer_size = file_scans.back()->buffer_manager->GetBuffer(0)->actual_size;
			current_boundary = CSVIterator(current_file_idx, 0, 0, 0, buffer_size);
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
	if (single_threaded) {
		return system_threads;
	}
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
	if (running_threads == 0) {
		FillRejectsTable();
		if (context.client_data->debug_set_max_line_length) {
			context.client_data->debug_max_line_length = file_scans[0]->error_handler->GetMaxLineLength();
		}
	}
}

void CSVGlobalState::FillRejectsTable() {
	auto &options = bind_data.options;

	if (!options.rejects_table_name.empty()) {
		auto limit = options.rejects_limit;

		auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_table_name);
		lock_guard<mutex> lock(rejects->write_lock);
		auto &table = rejects->GetTable(context);
		InternalAppender appender(context, table);

		for (auto &file : file_scans) {
			auto file_name = file->file_path;
			auto &errors = file->error_handler->errors;
			for (auto &error_info : errors) {
				if (error_info.second.type != CSVErrorType::CAST_ERROR) {
					// For now we only will use it for casting errors
					continue;
				}
				// short circuit if we already have too many rejects
				if (limit == 0 || rejects->count < limit) {
					if (limit != 0 && rejects->count >= limit) {
						break;
					}
					rejects->count++;
					auto error = &error_info.second;
					auto row_line = file->error_handler->GetLine(error_info.first);
					auto col_idx = error->column_idx;
					auto col_name = bind_data.return_names[col_idx];
					// Add the row to the rejects table
					appender.BeginRow();
					appender.Append(string_t(file_name));
					appender.Append(row_line);
					appender.Append(col_idx);
					appender.Append(string_t("\"" + col_name + "\""));
					appender.Append(error->row[col_idx]);

					if (!options.rejects_recovery_columns.empty()) {
						child_list_t<Value> recovery_key;
						for (auto &key_idx : options.rejects_recovery_column_ids) {
							// Figure out if the recovery key is valid.
							// If not, error out for real.
							auto &value = error->row[key_idx];
							if (value.IsNull()) {
								throw InvalidInputException("%s at line %llu in column %s. Parser options:\n%s ",
								                            "Could not parse recovery column", row_line, col_name,
								                            options.ToString());
							}
							recovery_key.emplace_back(bind_data.return_names[key_idx], value);
						}
						appender.Append(Value::STRUCT(recovery_key));
					}
					auto row_error_msg =
					    StringUtil::Format("Could not convert string '%s' to '%s'", error->row[col_idx].ToString(),
					                       file->types[col_idx].ToString());
					appender.Append(string_t(row_error_msg));
					appender.EndRow();
				}
				appender.Close();
			}
		}
	}
}

} // namespace duckdb
