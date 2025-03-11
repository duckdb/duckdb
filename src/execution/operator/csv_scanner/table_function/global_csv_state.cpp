#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"

namespace duckdb {

CSVGlobalState::CSVGlobalState(ClientContext &context_p, const CSVReaderOptions &options, idx_t total_file_count,
                               const MultiFileBindData &bind_data)
    : context(context_p), bind_data(bind_data), sniffer_mismatch_error(options.sniffer_user_mismatch_error) {
	// There are situations where we only support single threaded scanning
	auto system_threads = context.db->NumberOfThreads();
	bool many_csv_files = total_file_count > 1 && total_file_count > system_threads * 2;
	single_threaded = many_csv_files || !options.parallel;
	scanner_idx = 0;
	initialized = false;
}

void CSVGlobalState::FinishTask(CSVFileScan &scan) {
	auto started_tasks = scan.started_tasks.load();
	auto finished_tasks = ++scan.finished_tasks;
	if (finished_tasks == started_tasks) {
		// all scans finished for this file
		FinishFile(scan);
	} else if (finished_tasks > scan.started_tasks) {
		throw InternalException("Finished more tasks than were started for this file");
	}
}

void CSVGlobalState::FinishScan(unique_ptr<StringValueScanner> scanner) {
	if (!scanner) {
		return;
	}
	// We have to insert information for validation
	auto previous_file = scanner->csv_file_scan;
	previous_file->validator.Insert(scanner->scanner_idx, scanner->GetValidationLine());
	scanner.reset();
	FinishTask(*previous_file);
}

unique_ptr<StringValueScanner> CSVGlobalState::Next(shared_ptr<CSVFileScan> &current_file_ptr) {
	auto &current_file = *current_file_ptr;
	if (!initialized) {
		// initialize the boundary for this file
		current_boundary = current_file.start_iterator;
		current_boundary.SetCurrentBoundaryToPosition(single_threaded, current_file.options);
		if (current_boundary.done && context.client_data->debug_set_max_line_length) {
			context.client_data->debug_max_line_length =
			    MaxValue<idx_t>(context.client_data->debug_max_line_length, current_boundary.pos.buffer_pos);
		}
		current_buffer_in_use =
		    make_shared_ptr<CSVBufferUsage>(*current_file.buffer_manager, current_boundary.GetBufferIdx());
		initialized = true;
	} else {
		// produce the next boundary for this file
		if (current_boundary.done || !current_boundary.Next(*current_file.buffer_manager, current_file.options)) {
			// finished processing this file - return
			return nullptr;
		}
	}
	// create the scanner for this file
	if (current_buffer_in_use->buffer_idx != current_boundary.GetBufferIdx()) {
		current_buffer_in_use =
		    make_shared_ptr<CSVBufferUsage>(*current_file.buffer_manager, current_boundary.GetBufferIdx());
	}
	++current_file.started_tasks;
	// We first create the scanner for the current boundary
	auto csv_scanner =
	    make_uniq<StringValueScanner>(scanner_idx++, current_file.buffer_manager, current_file.state_machine,
	                                  current_file.error_handler, current_file_ptr, false, current_boundary);
	csv_scanner->buffer_tracker = current_buffer_in_use;
	// We initialize the scan
	return csv_scanner;
}

void CSVGlobalState::FinishLaunchingTasks(CSVFileScan &file) {
	initialized = false;
	current_buffer_in_use.reset();
	// we are finished launching tasks for this file
	// finish a task to indicate we can begin cleanup once all scans are done
	FinishTask(file);
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
		context.client_data->debug_max_line_length =
		    MaxValue<idx_t>(context.client_data->debug_max_line_length, scan.error_handler->GetMaxLineLength());
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

void CSVGlobalState::FillRejectsTable(CSVFileScan &scan) {
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

	// get the file indexes for the rejects table
	// we store these so that they are deterministic (i.e. file index 0 always gets the lowest rejects index)
	// otherwise parallelism can result in out-of-order file indexes
	auto file_idx = scan.GetFileIndex();
	for (idx_t i = rejects_file_indexes.size(); i <= file_idx; i++) {
		rejects_file_indexes.push_back(rejects->GetCurrentFileIndex(scan_idx));
	}
	const idx_t rejects_file_idx = rejects_file_indexes[file_idx];
	scan.error_handler->FillRejectsTable(errors_appender, rejects_file_idx, scan_idx, scan, *rejects, bind_data, limit);
	if (rejects->count != 0) {
		rejects->count = 0;
		FillScanErrorTable(scans_appender, scan_idx, rejects_file_idx, scan);
	}
	errors_appender.Close();
	scans_appender.Close();
}

} // namespace duckdb
