#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<ColumnIndex> column_ids_p, const ReadCSVData &bind_data_p)
    : context(context_p), system_threads(system_threads_p), column_ids(std::move(column_ids_p)),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error), bind_data(bind_data_p) {

	if (buffer_manager && buffer_manager->GetFilePath() == files[0]) {
		auto state_machine = make_shared_ptr<CSVStateMachine>(
		    CSVStateMachineCache::Get(context).Get(options.dialect_options.state_machine_options), options);
		// If we already have a buffer manager, we don't need to reconstruct it to the first file
		file_scans.emplace_back(make_uniq<CSVFileScan>(context, buffer_manager, state_machine, options, bind_data,
		                                               column_ids, file_schema));
	} else {
		// If not we need to construct it for the first file
		file_scans.emplace_back(
		    make_uniq<CSVFileScan>(context, files[0], options, 0U, bind_data, column_ids, file_schema, false));
	}
	idx_t cur_file_idx = 0;
	while (file_scans.back()->start_iterator.done && file_scans.size() < files.size()) {
		cur_file_idx++;
		file_scans.emplace_back(make_uniq<CSVFileScan>(context, files[cur_file_idx], options, cur_file_idx, bind_data,
		                                               column_ids, file_schema, false));
	}
	// There are situations where we only support single threaded scanning
	bool many_csv_files = files.size() > 1 && files.size() > system_threads * 2;
	single_threaded = many_csv_files || !options.parallel;
	last_file_idx = 0;
	scanner_idx = 0;
	running_threads = CSVGlobalState::MaxThreads();
	current_boundary = file_scans.back()->start_iterator;
	current_boundary.SetCurrentBoundaryToPosition(single_threaded, options);
	if (current_boundary.done && context.client_data->debug_set_max_line_length) {
		context.client_data->debug_max_line_length = current_boundary.pos.buffer_pos;
	}
	current_buffer_in_use =
	    make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager, current_boundary.GetBufferIdx());
}

bool CSVGlobalState::IsDone() const {
	lock_guard<mutex> parallel_lock(main_mutex);
	return current_boundary.done;
}

double CSVGlobalState::GetProgress(const ReadCSVData &bind_data_p) const {
	lock_guard<mutex> parallel_lock(main_mutex);
	idx_t total_files = bind_data.files.size();
	// get the progress WITHIN the current file
	double percentage = 0;
	if (file_scans.front()->file_size == 0) {
		percentage = 1.0;
	} else {
		// for compressed files, read bytes may greater than files size.
		for (auto &file : file_scans) {
			double file_progress;
			if (!file->buffer_manager) {
				// We are done with this file, so it's 100%
				file_progress = 1.0;
			} else if (file->buffer_manager->file_handle->compression_type == FileCompressionType::GZIP ||
			           file->buffer_manager->file_handle->compression_type == FileCompressionType::ZSTD) {
				// This file is not done, and is a compressed file
				file_progress = file->buffer_manager->file_handle->GetProgress();
			} else {
				file_progress = static_cast<double>(file->bytes_read);
			}
			// This file is an uncompressed file, so we use the more price bytes_read from the scanner
			percentage += (static_cast<double>(1) / static_cast<double>(total_files)) *
			              std::min(1.0, file_progress / static_cast<double>(file->file_size));
		}
	}
	return percentage * 100;
}

unique_ptr<StringValueScanner> CSVGlobalState::Next(optional_ptr<StringValueScanner> previous_scanner) {
	if (previous_scanner) {
		// We have to insert information for validation
		lock_guard<mutex> parallel_lock(main_mutex);
		validator.Insert(previous_scanner->csv_file_scan->file_idx, previous_scanner->scanner_idx,
		                 previous_scanner->GetValidationLine());
	}
	if (single_threaded) {
		{
			lock_guard<mutex> parallel_lock(main_mutex);
			if (previous_scanner) {
				// Cleanup previous scanner.
				previous_scanner->buffer_tracker.reset();
				current_buffer_in_use.reset();
				previous_scanner->csv_file_scan->Finish();
			}
		}
		idx_t cur_idx;
		bool empty_file = false;
		do {
			{
				lock_guard<mutex> parallel_lock(main_mutex);
				cur_idx = last_file_idx++;
				if (cur_idx >= bind_data.files.size()) {
					// No more files to scan
					return nullptr;
				}
				if (cur_idx == 0) {
					D_ASSERT(!previous_scanner);
					auto current_file = file_scans.front();
					return make_uniq<StringValueScanner>(scanner_idx++, current_file->buffer_manager,
					                                     current_file->state_machine, current_file->error_handler,
					                                     current_file, false, current_boundary);
				}
			}
			auto file_scan = make_shared_ptr<CSVFileScan>(context, bind_data.files[cur_idx], bind_data.options, cur_idx,
			                                              bind_data, column_ids, file_schema, true);
			empty_file = file_scan->file_size == 0;

			if (!empty_file) {
				lock_guard<mutex> parallel_lock(main_mutex);
				file_scans.emplace_back(std::move(file_scan));
				auto current_file = file_scans.back();
				current_boundary = current_file->start_iterator;
				current_boundary.SetCurrentBoundaryToPosition(single_threaded, bind_data.options);
				current_buffer_in_use = make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager,
				                                                        current_boundary.GetBufferIdx());

				return make_uniq<StringValueScanner>(scanner_idx++, current_file->buffer_manager,
				                                     current_file->state_machine, current_file->error_handler,
				                                     current_file, false, current_boundary);
			}
		} while (empty_file);
	}
	lock_guard<mutex> parallel_lock(main_mutex);
	if (finished) {
		return nullptr;
	}
	if (current_buffer_in_use->buffer_idx != current_boundary.GetBufferIdx()) {
		current_buffer_in_use =
		    make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager, current_boundary.GetBufferIdx());
	}
	// We first create the scanner for the current boundary
	auto &current_file = *file_scans.back();
	auto csv_scanner =
	    make_uniq<StringValueScanner>(scanner_idx++, current_file.buffer_manager, current_file.state_machine,
	                                  current_file.error_handler, file_scans.back(), false, current_boundary);
	threads_per_file[csv_scanner->csv_file_scan->file_idx]++;
	if (previous_scanner) {
		threads_per_file[previous_scanner->csv_file_scan->file_idx]--;
		if (threads_per_file[previous_scanner->csv_file_scan->file_idx] == 0) {
			previous_scanner->buffer_tracker.reset();
			previous_scanner->csv_file_scan->Finish();
		}
	}
	csv_scanner->buffer_tracker = current_buffer_in_use;

	// We then produce the next boundary
	if (!current_boundary.Next(*current_file.buffer_manager, bind_data.options)) {
		// This means we are done scanning the current file
		do {
			auto current_file_idx = file_scans.back()->file_idx + 1;
			if (current_file_idx < bind_data.files.size()) {
				// If we have a next file we have to construct the file scan for that
				file_scans.emplace_back(make_shared_ptr<CSVFileScan>(context, bind_data.files[current_file_idx],
				                                                     bind_data.options, current_file_idx, bind_data,
				                                                     column_ids, file_schema, false));
				// And re-start the boundary-iterator
				current_boundary = file_scans.back()->start_iterator;
				current_boundary.SetCurrentBoundaryToPosition(single_threaded, bind_data.options);
				current_buffer_in_use = make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager,
				                                                        current_boundary.GetBufferIdx());
			} else {
				// If not we are done with this CSV Scanning
				finished = true;
				break;
			}
		} while (current_boundary.done);
	}
	// We initialize the scan
	return csv_scanner;
}

idx_t CSVGlobalState::MaxThreads() const {
	// We initialize max one thread per our set bytes per thread limit
	if (single_threaded || !file_scans.front()->on_disk_file) {
		return system_threads;
	}
	const idx_t bytes_per_thread = CSVIterator::BytesPerThread(file_scans.front()->options);
	const idx_t total_threads = file_scans.front()->file_size / bytes_per_thread + 1;
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
		const bool ignore_or_store_errors =
		    bind_data.options.ignore_errors.GetValue() || bind_data.options.store_rejects.GetValue();
		if (!single_threaded && !ignore_or_store_errors) {
			// If we are running multithreaded and not ignoring errors, we must run the validator
			validator.Verify();
		}
		for (const auto &file : file_scans) {
			file->error_handler->ErrorIfNeeded();
		}
		FillRejectsTable();
		if (context.client_data->debug_set_max_line_length) {
			context.client_data->debug_max_line_length = file_scans[0]->error_handler->GetMaxLineLength();
		}
	}
}

void FillScanErrorTable(InternalAppender &scan_appender, idx_t scan_idx, idx_t file_idx, CSVFileScan &file) {
	CSVReaderOptions &options = file.options;
	// Add the row to the rejects table
	scan_appender.BeginRow();
	// 1. Scan Idx
	scan_appender.Append(scan_idx);
	// 2. File Idx
	scan_appender.Append(file_idx);
	// 3. File Path
	scan_appender.Append(string_t(file.file_path));
	// 4. Delimiter
	scan_appender.Append(string_t(options.dialect_options.state_machine_options.delimiter.FormatValue()));
	// 5. Quote
	scan_appender.Append(string_t(options.dialect_options.state_machine_options.quote.FormatValue()));
	// 6. Escape
	scan_appender.Append(string_t(options.dialect_options.state_machine_options.escape.FormatValue()));
	// 7. NewLine Delimiter
	scan_appender.Append(string_t(options.NewLineIdentifierToString()));
	// 8. Skip Rows
	scan_appender.Append(Value::UINTEGER(NumericCast<uint32_t>(options.dialect_options.skip_rows.GetValue())));
	// 9. Has Header
	scan_appender.Append(Value::BOOLEAN(options.dialect_options.header.GetValue()));

	auto &types = file.GetTypes();
	auto &names = file.GetNames();

	// 10. List<Struct<Column-Name:Types>> {'col1': 'INTEGER', 'col2': 'VARCHAR'}
	std::ostringstream columns;
	columns << "{";
	for (idx_t i = 0; i < types.size(); i++) {
		columns << "'" << names[i] << "': '" << types[i].ToString() << "'";
		if (i != types.size() - 1) {
			columns << ",";
		}
	}
	columns << "}";
	scan_appender.Append(string_t(columns.str()));
	// 11. Date Format
	auto date_format = options.dialect_options.date_format[LogicalType::DATE].GetValue();
	if (!date_format.Empty()) {
		scan_appender.Append(string_t(date_format.format_specifier));
	} else {
		scan_appender.Append(Value());
	}

	// 12. Timestamp Format
	auto timestamp_format = options.dialect_options.date_format[LogicalType::TIMESTAMP].GetValue();
	if (!timestamp_format.Empty()) {
		scan_appender.Append(string_t(timestamp_format.format_specifier));
	} else {
		scan_appender.Append(Value());
	}

	// 13. The Extra User Arguments
	if (options.user_defined_parameters.empty()) {
		scan_appender.Append(Value());
	} else {
		scan_appender.Append(string_t(options.user_defined_parameters));
	}
	// Finish the row to the rejects table
	scan_appender.EndRow();
}

void CSVGlobalState::FillRejectsTable() const {
	auto &options = bind_data.options;

	if (options.store_rejects.GetValue()) {
		auto limit = options.rejects_limit;
		auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_scan_name.GetValue(),
		                                            options.rejects_table_name.GetValue());
		lock_guard<mutex> lock(rejects->write_lock);
		auto &errors_table = rejects->GetErrorsTable(context);
		auto &scans_table = rejects->GetScansTable(context);
		InternalAppender errors_appender(context, errors_table);
		InternalAppender scans_appender(context, scans_table);
		idx_t scan_idx = context.transaction.GetActiveQuery();
		for (auto &file : file_scans) {
			const idx_t file_idx = rejects->GetCurrentFileIndex(scan_idx);
			auto file_name = file->file_path;
			file->error_handler->FillRejectsTable(errors_appender, file_idx, scan_idx, *file, *rejects, bind_data,
			                                      limit);
			if (rejects->count != 0) {
				rejects->count = 0;
				FillScanErrorTable(scans_appender, scan_idx, file_idx, *file);
			}
		}
		errors_appender.Close();
		scans_appender.Close();
	}
}

} // namespace duckdb
