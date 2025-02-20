#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"

namespace duckdb {

shared_ptr<CSVFileScan> CSVGlobalState::CreateFileScan(idx_t file_idx, shared_ptr<CSVBufferManager> buffer_manager) {
	if (file_idx == 0 && bind_data.initial_reader) {
		throw InternalException("FIXME: this should have been handled before");
	}
	auto multi_file_reader = MultiFileReader::CreateDefault("CSV Scan");

	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	auto &file_name = files[file_idx];
	auto options = csv_data.options;
	shared_ptr<BaseFileReader> csv_file_scan;
	if (file_idx < bind_data.union_readers.size()) {
		if (buffer_manager) {
			throw InternalException("buffer_manager / single threaded scan for union reader");
		}
		csv_file_scan = CSVMultiFileInfo::CreateReader(context, *this, *bind_data.union_readers[file_idx], bind_data);
	} else if (file_idx < csv_data.column_info.size()) {
		// Serialized union by name - sniff again
		auto &csv_names = csv_data.column_info[file_idx].names;
		auto &csv_types = csv_data.column_info[file_idx].types;
		options.dialect_options.num_cols = csv_names.size();
		csv_file_scan =
		    make_uniq<CSVFileScan>(context, file_name, std::move(options), bind_data.file_options, csv_names, csv_types,
		                           file_schema, single_threaded, std::move(buffer_manager), true);
	} else {
		csv_file_scan = CSVMultiFileInfo::CreateReader(context, *this, file_name, file_idx, bind_data);
	}
	csv_file_scan->reader_data.file_list_idx = file_idx;

	multi_file_reader->InitializeReader(*csv_file_scan, bind_data.file_options, bind_data.reader_bind, global_columns,
	                                    column_ids, nullptr, file_name, context, nullptr);
	CSVMultiFileInfo::FinalizeReader(context, *csv_file_scan);
	return shared_ptr_cast<BaseFileReader, CSVFileScan>(std::move(csv_file_scan));
}

CSVGlobalState::CSVGlobalState(ClientContext &context_p, const shared_ptr<CSVBufferManager> &buffer_manager,
                               const CSVReaderOptions &options, idx_t system_threads_p, const vector<string> &files_p,
                               vector<ColumnIndex> column_ids_p, MultiFileBindData &bind_data_p)
    : context(context_p), files(files_p), system_threads(system_threads_p), column_ids(std::move(column_ids_p)),
      sniffer_mismatch_error(options.sniffer_user_mismatch_error), bind_data(bind_data_p) {
	global_columns = MultiFileReaderColumnDefinition::ColumnsFromNamesAndTypes(bind_data.names, bind_data.types);

	shared_ptr<CSVFileScan> initial_reader;
	if (bind_data.initial_reader) {
		initial_reader = shared_ptr_cast<BaseFileReader, CSVFileScan>(std::move(bind_data.initial_reader));
	} else if (buffer_manager && buffer_manager->GetFilePath() == files[0]) {
		// If we already have a buffer manager, we don't need to reconstruct it to the first file
		initial_reader = CreateFileScan(0, buffer_manager);
	} else {
		// If not we need to construct it for the first file
		initial_reader = CreateFileScan(0);
	}
	file_scans.emplace_back(std::move(initial_reader));

	idx_t cur_file_idx = 0;
	while (file_scans.back()->start_iterator.done && file_scans.size() < files.size()) {
		cur_file_idx++;
		file_scans.emplace_back(CreateFileScan(cur_file_idx));
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
	idx_t total_files = files.size();
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

unique_ptr<StringValueScanner> CSVGlobalState::Next(unique_ptr<StringValueScanner> previous_scanner) {
	unique_lock<mutex> parallel_lock(main_mutex);
	if (previous_scanner) {
		// We have to insert information for validation
		auto &file = *previous_scanner->csv_file_scan;
		file.validator.Insert(previous_scanner->scanner_idx, previous_scanner->GetValidationLine());
		previous_scanner.reset();
		auto finished_tasks = ++file.finished_tasks;
		if (file.finished_scan && finished_tasks == file.started_tasks) {
			// all scans finished for this file
			FinishFile(file);
		}
	}
	if (single_threaded) {
		idx_t cur_idx;
		while (true) {
			cur_idx = last_file_idx++;
			if (cur_idx >= files.size()) {
				// No more files to scan
				return nullptr;
			}
			if (cur_idx == 0) {
				D_ASSERT(!previous_scanner);
				auto current_file = file_scans.front();
				++current_file->started_tasks;
				return make_uniq<StringValueScanner>(scanner_idx++, current_file->buffer_manager,
				                                     current_file->state_machine, current_file->error_handler,
				                                     current_file, false, current_boundary);
			}
			current_buffer_in_use.reset();
			parallel_lock.unlock();
			auto file_scan = CreateFileScan(cur_idx);
			parallel_lock.lock();
			bool empty_file = file_scan->file_size == 0;

			if (!empty_file) {
				auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
				file_scans.emplace_back(std::move(file_scan));
				auto current_file = file_scans.back();
				current_boundary = current_file->start_iterator;
				current_boundary.SetCurrentBoundaryToPosition(single_threaded, csv_data.options);
				current_buffer_in_use = make_shared_ptr<CSVBufferUsage>(*file_scans.back()->buffer_manager,
				                                                        current_boundary.GetBufferIdx());
				++current_file->started_tasks;
				return make_uniq<StringValueScanner>(scanner_idx++, current_file->buffer_manager,
				                                     current_file->state_machine, current_file->error_handler,
				                                     current_file, false, current_boundary);
			}
		}
	}
	if (finished) {
		return nullptr;
	}
	auto &current_file = *file_scans.back();
	if (current_buffer_in_use->buffer_idx != current_boundary.GetBufferIdx()) {
		current_buffer_in_use =
		    make_shared_ptr<CSVBufferUsage>(*current_file.buffer_manager, current_boundary.GetBufferIdx());
	}
	// We first create the scanner for the current boundary
	auto csv_scanner =
	    make_uniq<StringValueScanner>(scanner_idx++, current_file.buffer_manager, current_file.state_machine,
	                                  current_file.error_handler, file_scans.back(), false, current_boundary);
	csv_scanner->buffer_tracker = current_buffer_in_use;

	// We then produce the next boundary
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	++current_file.started_tasks;
	if (!current_boundary.Next(*current_file.buffer_manager, csv_data.options)) {
		// This means we are done scanning the current file
		current_file.finished_scan = true;
		do {
			auto current_file_idx = file_scans.back()->GetFileIndex() + 1;
			if (current_file_idx < files.size()) {
				// If we have a next file we have to construct the file scan for that
				file_scans.emplace_back(CreateFileScan(current_file_idx));
				// And re-start the boundary-iterator
				current_boundary = file_scans.back()->start_iterator;
				current_boundary.SetCurrentBoundaryToPosition(single_threaded, csv_data.options);
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

void CSVGlobalState::FinishFile(CSVFileScan &scan) {
	if (current_buffer_in_use && RefersToSameObject(current_buffer_in_use->buffer_manager, *scan.buffer_manager)) {
		current_buffer_in_use.reset();
	}
	scan.Finish();
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	const bool ignore_or_store_errors =
	    csv_data.options.ignore_errors.GetValue() || csv_data.options.store_rejects.GetValue();
	if (!single_threaded && !ignore_or_store_errors) {
		// If we are running multithreaded and not ignoring errors, we must run the validator
		scan.validator.Verify();
	}
	scan.error_handler->ErrorIfAny();
	FillRejectsTable(scan);
	if (context.client_data->debug_set_max_line_length) {
		context.client_data->debug_max_line_length = scan.error_handler->GetMaxLineLength();
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
	scan_appender.Append(string_t(file.GetFileName()));
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
		auto parameters = options.GetUserDefinedParameters();
		scan_appender.Append(string_t(parameters));
	}
	// Finish the row to the rejects table
	scan_appender.EndRow();
}

void CSVGlobalState::FillRejectsTable(CSVFileScan &scan) const {
	auto &csv_data = bind_data.bind_data->Cast<ReadCSVData>();
	auto &options = csv_data.options;

	if (!options.store_rejects.GetValue()) {
		return;
	}
	auto limit = options.rejects_limit;
	auto rejects = CSVRejectsTable::GetOrCreate(context, options.rejects_scan_name.GetValue(),
	                                            options.rejects_table_name.GetValue());
	lock_guard<mutex> lock(rejects->write_lock);
	auto &errors_table = rejects->GetErrorsTable(context);
	auto &scans_table = rejects->GetScansTable(context);
	InternalAppender errors_appender(context, errors_table);
	InternalAppender scans_appender(context, scans_table);
	idx_t scan_idx = context.transaction.GetActiveQuery();

	const idx_t file_idx = rejects->GetCurrentFileIndex(scan_idx);
	scan.error_handler->FillRejectsTable(errors_appender, file_idx, scan_idx, scan, *rejects, bind_data, limit);
	if (rejects->count != 0) {
		rejects->count = 0;
		FillScanErrorTable(scans_appender, scan_idx, file_idx, scan);
	}
	errors_appender.Close();
	scans_appender.Close();
}

} // namespace duckdb
