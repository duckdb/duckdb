#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"

#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files,
                               vector<column_t> column_ids_p, const ReadCSVData &bind_data_p)
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
	};
	// There are situations where we only support single threaded scanning
	bool many_csv_files = files.size() > 1 && files.size() > system_threads * 2;
	single_threaded = many_csv_files || !options.parallel;
	last_file_idx = 0;
	scanner_idx = 0;
	running_threads = MaxThreads();
	current_boundary = file_scans.back()->start_iterator;
	current_boundary.SetCurrentBoundaryToPosition(single_threaded);
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
		// for compressed files, readed bytes may greater than files size.
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
			percentage += (double(1) / double(total_files)) * std::min(1.0, file_progress / double(file->file_size));
		}
	}
	return percentage * 100;
}

unique_ptr<StringValueScanner> CSVGlobalState::Next(optional_ptr<StringValueScanner> previous_scanner) {
	if (single_threaded) {
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
				current_boundary.SetCurrentBoundaryToPosition(single_threaded);
				current_buffer_in_use = make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager,
				                                                        current_boundary.GetBufferIdx());
				if (previous_scanner) {
					previous_scanner->buffer_tracker.reset();
					current_buffer_in_use.reset();
					previous_scanner->csv_file_scan->Finish();
				}
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
	if (!current_boundary.Next(*current_file.buffer_manager)) {
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
				current_boundary.SetCurrentBoundaryToPosition(single_threaded);
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
	if (single_threaded) {
		return system_threads;
	}
	idx_t total_threads = file_scans.front()->file_size / CSVIterator::BYTES_PER_THREAD + 1;

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
		for (auto &file : file_scans) {
			file->error_handler->ErrorIfNeeded();
		}
		FillRejectsTable();
		if (context.client_data->debug_set_max_line_length) {
			context.client_data->debug_max_line_length = file_scans[0]->error_handler->GetMaxLineLength();
		}
	}
}

bool IsCSVErrorAcceptedReject(CSVErrorType type) {
	switch (type) {
	case CSVErrorType::CAST_ERROR:
	case CSVErrorType::TOO_MANY_COLUMNS:
	case CSVErrorType::TOO_FEW_COLUMNS:
	case CSVErrorType::MAXIMUM_LINE_SIZE:
	case CSVErrorType::UNTERMINATED_QUOTES:
	case CSVErrorType::INVALID_UNICODE:
		return true;
	default:
		return false;
	}
}

string CSVErrorTypeToEnum(CSVErrorType type) {
	switch (type) {
	case CSVErrorType::CAST_ERROR:
		return "CAST";
	case CSVErrorType::TOO_FEW_COLUMNS:
		return "MISSING COLUMNS";
	case CSVErrorType::TOO_MANY_COLUMNS:
		return "TOO MANY COLUMNS";
	case CSVErrorType::MAXIMUM_LINE_SIZE:
		return "LINE SIZE OVER MAXIMUM";
	case CSVErrorType::UNTERMINATED_QUOTES:
		return "UNQUOTED VALUE";
	case CSVErrorType::INVALID_UNICODE:
		return "INVALID UNICODE";
	default:
		throw InternalException("CSV Error is not valid to be stored in a Rejects Table");
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
	// 10. List<Struct<Column-Name:Types>> {'col1': 'INTEGER', 'col2': 'VARCHAR'}
	std::ostringstream columns;
	columns << "{";
	for (idx_t i = 0; i < file.types.size(); i++) {
		columns << "'" << file.names[i] << "': '" << file.types[i].ToString() << "'";
		if (i != file.types.size() - 1) {
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

void CSVGlobalState::FillRejectsTable() {
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
			idx_t file_idx = rejects->GetCurrentFileIndex(scan_idx);
			auto file_name = file->file_path;
			auto &errors = file->error_handler->errors;
			// We first insert the file into the file scans table
			for (auto &error_vector : errors) {
				for (auto &error : error_vector.second) {
					if (!IsCSVErrorAcceptedReject(error.type)) {
						continue;
					}
					// short circuit if we already have too many rejects
					if (limit == 0 || rejects->count < limit) {
						if (limit != 0 && rejects->count >= limit) {
							break;
						}
						rejects->count++;
						auto row_line = file->error_handler->GetLine(error.error_info);
						auto col_idx = error.column_idx;
						// Add the row to the rejects table
						errors_appender.BeginRow();
						// 1. Scan Id
						errors_appender.Append(scan_idx);
						// 2. File Id
						errors_appender.Append(file_idx);
						// 3. Row Line
						errors_appender.Append(row_line);
						// 4. Byte Position of the row error
						errors_appender.Append(error.row_byte_position + 1);
						// 5. Byte Position where error occurred
						if (!error.byte_position.IsValid()) {
							// This means this error comes from a flush, and we don't support this yet, so we give it
							// a null
							errors_appender.Append(Value());
						} else {
							errors_appender.Append(error.byte_position.GetIndex() + 1);
						}
						// 6. Column Index
						if (error.type == CSVErrorType::MAXIMUM_LINE_SIZE) {
							errors_appender.Append(Value());
						} else {
							errors_appender.Append(col_idx + 1);
						}
						// 7. Column Name (If Applicable)
						switch (error.type) {
						case CSVErrorType::TOO_MANY_COLUMNS:
						case CSVErrorType::MAXIMUM_LINE_SIZE:
							errors_appender.Append(Value());
							break;
						case CSVErrorType::TOO_FEW_COLUMNS:
							D_ASSERT(bind_data.return_names.size() > col_idx + 1);
							errors_appender.Append(string_t(bind_data.return_names[col_idx + 1]));
							break;
						default:
							errors_appender.Append(string_t(bind_data.return_names[col_idx]));
						}
						// 8. Error Type
						errors_appender.Append(string_t(CSVErrorTypeToEnum(error.type)));
						// 9. Original CSV Line
						errors_appender.Append(string_t(error.csv_row));
						// 10. Full Error Message
						errors_appender.Append(string_t(error.error_message));
						errors_appender.EndRow();
					}
				}
			}
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
