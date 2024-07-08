#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

CSVSniffer::CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
                       CSVStateMachineCache &state_machine_cache_p, bool default_null_to_varchar_p)
    : state_machine_cache(state_machine_cache_p), options(options_p), buffer_manager(std::move(buffer_manager_p)),
      default_null_to_varchar(default_null_to_varchar_p) {
	// Initialize Format Candidates
	for (const auto &format_template : format_template_candidates) {
		auto &logical_type = format_template.first;
		best_format_candidates[logical_type].clear();
	}
	// Initialize max columns found to either 0 or however many were set
	max_columns_found = set_columns.Size();
	error_handler = make_shared_ptr<CSVErrorHandler>(options.ignore_errors.GetValue());
	detection_error_handler = make_shared_ptr<CSVErrorHandler>(true);
	if (options.columns_set) {
		set_columns = SetColumns(&options.sql_type_list, &options.name_list);
	}
}

bool SetColumns::IsSet() {
	if (!types) {
		return false;
	}
	return !types->empty();
}

idx_t SetColumns::Size() {
	if (!types) {
		return 0;
	}
	return types->size();
}

template <class T>
void MatchAndReplace(CSVOption<T> &original, CSVOption<T> &sniffed, const string &name, string &error) {
	if (original.IsSetByUser()) {
		// We verify that the user input matches the sniffed value
		if (original != sniffed) {
			error += "CSV Sniffer: Sniffer detected value different than the user input for the " + name;
			error += " options \n Set: " + original.FormatValue() + " Sniffed: " + sniffed.FormatValue() + "\n";
		}
	} else {
		// We replace the value of original with the sniffed value
		original.Set(sniffed.GetValue(), false);
	}
}
void MatchAndRepaceUserSetVariables(DialectOptions &original, DialectOptions &sniffed, string &error, bool found_date,
                                    bool found_timestamp) {
	MatchAndReplace(original.header, sniffed.header, "Header", error);
	if (sniffed.state_machine_options.new_line.GetValue() != NewLineIdentifier::NOT_SET) {
		// Is sniffed line is not set (e.g., single-line file) , we don't try to replace and match.
		MatchAndReplace(original.state_machine_options.new_line, sniffed.state_machine_options.new_line, "New Line",
		                error);
	}
	MatchAndReplace(original.skip_rows, sniffed.skip_rows, "Skip Rows", error);
	MatchAndReplace(original.state_machine_options.delimiter, sniffed.state_machine_options.delimiter, "Delimiter",
	                error);
	MatchAndReplace(original.state_machine_options.quote, sniffed.state_machine_options.quote, "Quote", error);
	MatchAndReplace(original.state_machine_options.escape, sniffed.state_machine_options.escape, "Escape", error);
	if (found_date) {
		MatchAndReplace(original.date_format[LogicalTypeId::DATE], sniffed.date_format[LogicalTypeId::DATE],
		                "Date Format", error);
	}
	if (found_timestamp) {
		MatchAndReplace(original.date_format[LogicalTypeId::TIMESTAMP], sniffed.date_format[LogicalTypeId::TIMESTAMP],
		                "Timestamp Format", error);
	}
}
// Set the CSV Options in the reference
void CSVSniffer::SetResultOptions() {
	bool found_date = false;
	bool found_timestamp = false;
	for (auto &type : detected_types) {
		if (type == LogicalType::DATE) {
			found_date = true;
		} else if (type == LogicalType::TIMESTAMP) {
			found_timestamp = true;
		}
	}
	MatchAndRepaceUserSetVariables(options.dialect_options, best_candidate->GetStateMachine().dialect_options,
	                               options.sniffer_user_mismatch_error, found_date, found_timestamp);
	options.dialect_options.num_cols = best_candidate->GetStateMachine().dialect_options.num_cols;
}

SnifferResult CSVSniffer::MinimalSniff() {
	if (set_columns.IsSet()) {
		// Nothing to see here
		return SnifferResult(*set_columns.types, *set_columns.names);
	}
	// Return Types detected
	vector<LogicalType> return_types;
	// Column Names detected
	vector<string> names;

	buffer_manager->sniffing = true;
	const idx_t result_size = 2;

	auto state_machine =
	    make_shared_ptr<CSVStateMachine>(options, options.dialect_options.state_machine_options, state_machine_cache);
	ColumnCountScanner count_scanner(buffer_manager, state_machine, error_handler, result_size);
	auto &sniffed_column_counts = count_scanner.ParseChunk();
	if (sniffed_column_counts.result_position == 0) {
		return {{}, {}};
	}

	state_machine->dialect_options.num_cols = sniffed_column_counts[0];
	options.dialect_options.num_cols = sniffed_column_counts[0];

	// First figure out the number of columns on this configuration
	auto scanner = count_scanner.UpgradeToStringValueScanner();
	// Parse chunk and read csv with info candidate
	auto &data_chunk = scanner->ParseChunk().ToChunk();
	idx_t start_row = 0;
	if (sniffed_column_counts.result_position == 2) {
		// If equal to two, we will only use the second row for type checking
		start_row = 1;
	}

	// Gather Types
	for (idx_t i = 0; i < state_machine->dialect_options.num_cols; i++) {
		best_sql_types_candidates_per_column_idx[i] = state_machine->options.auto_type_candidates;
	}
	SniffTypes(data_chunk, *state_machine, best_sql_types_candidates_per_column_idx, start_row);

	// Possibly Gather Header
	vector<HeaderValue> potential_header;
	if (start_row != 0) {
		for (idx_t col_idx = 0; col_idx < data_chunk.ColumnCount(); col_idx++) {
			auto &cur_vector = data_chunk.data[col_idx];
			auto vector_data = FlatVector::GetData<string_t>(cur_vector);
			HeaderValue val(vector_data[0]);
			potential_header.emplace_back(val);
		}
	}
	names = DetectHeaderInternal(buffer_manager->context, potential_header, *state_machine, set_columns,
	                             best_sql_types_candidates_per_column_idx, options, *error_handler);

	for (idx_t column_idx = 0; column_idx < best_sql_types_candidates_per_column_idx.size(); column_idx++) {
		LogicalType d_type = best_sql_types_candidates_per_column_idx[column_idx].back();
		if (best_sql_types_candidates_per_column_idx[column_idx].size() == options.auto_type_candidates.size()) {
			d_type = LogicalType::VARCHAR;
		}
		detected_types.push_back(d_type);
	}

	return {detected_types, names};
}

SnifferResult CSVSniffer::AdaptiveSniff(CSVSchema &file_schema) {
	auto min_sniff_res = MinimalSniff();
	bool run_full = error_handler->AnyErrors() || detection_error_handler->AnyErrors();
	// Check if we are happy with the result or if we need to do more sniffing
	if (!error_handler->AnyErrors() && !detection_error_handler->AnyErrors()) {
		// If we got no errors, we also run full if schemas do not match.
		if (!set_columns.IsSet() && !options.file_options.AnySet()) {
			string error;
			run_full =
			    !file_schema.SchemasMatch(error, min_sniff_res.names, min_sniff_res.return_types, options.file_path);
		}
	}
	if (run_full) {
		// We run full sniffer
		string error;
		auto full_sniffer = SniffCSV();
		if (!set_columns.IsSet() && !options.file_options.AnySet()) {
			if (!file_schema.SchemasMatch(error, full_sniffer.names, full_sniffer.return_types, options.file_path)) {
				throw InvalidInputException(error);
			}
		}
		return full_sniffer;
	}
	return min_sniff_res;
}
SnifferResult CSVSniffer::SniffCSV(bool force_match) {
	buffer_manager->sniffing = true;
	// 1. Dialect Detection
	DetectDialect();
	// 2. Type Detection
	DetectTypes();
	// 3. Type Refinement
	RefineTypes();
	// 4. Header Detection
	DetectHeader();
	// 5. Type Replacement
	ReplaceTypes();

	// We reset the buffer for compressed files
	// This is done because we can't easily seek on compressed files, if a buffer goes out of scope we must read from
	// the start
	if (!buffer_manager->file_handle->uncompressed) {
		buffer_manager->ResetBufferManager();
	}
	buffer_manager->sniffing = false;
	if (!best_candidate->error_handler->errors.empty() && !options.ignore_errors.GetValue()) {
		for (auto &error_vector : best_candidate->error_handler->errors) {
			for (auto &error : error_vector.second) {
				if (error.type == CSVErrorType::MAXIMUM_LINE_SIZE) {
					// If it's a maximum line size error, we can do it now.
					error_handler->Error(error);
				}
			}
		}
		auto error = CSVError::SniffingError(options.file_path);
		error_handler->Error(error);
	}
	D_ASSERT(best_sql_types_candidates_per_column_idx.size() == names.size());
	// We are done, Set the CSV Options in the reference. Construct and return the result.
	SetResultOptions();
	options.auto_detect = true;
	// Check if everything matches
	auto &error = options.sniffer_user_mismatch_error;
	if (set_columns.IsSet()) {
		bool match = true;
		// Columns and their types were set, let's validate they match
		if (options.dialect_options.header.GetValue()) {
			// If the header exists it should match
			string header_error = "The Column names set by the user do not match the ones found by the sniffer. \n";
			auto &set_names = *set_columns.names;
			for (idx_t i = 0; i < set_columns.Size(); i++) {
				if (set_names[i] != names[i]) {
					header_error += "Column at position: " + to_string(i) + " Set name: " + set_names[i] +
					                " Sniffed Name: " + names[i] + "\n";
					match = false;
				}
			}
			if (!match) {
				error += header_error;
			}
		}
		match = true;
		string type_error = "The Column types set by the user do not match the ones found by the sniffer. \n";
		auto &set_types = *set_columns.types;
		for (idx_t i = 0; i < set_columns.Size(); i++) {
			if (set_types[i] != detected_types[i]) {
				type_error += "Column at position: " + to_string(i) + " Set type: " + set_types[i].ToString() +
				              " Sniffed type: " + detected_types[i].ToString() + "\n";
				detected_types[i] = set_types[i];
				manually_set[i] = true;
				match = false;
			}
		}
		if (!match) {
			error += type_error;
		}

		if (!error.empty() && force_match) {
			throw InvalidInputException(error);
		}
		options.was_type_manually_set = manually_set;
	}
	if (!error.empty() && force_match) {
		throw InvalidInputException(error);
	}
	options.was_type_manually_set = manually_set;
	if (set_columns.IsSet()) {
		return SnifferResult(*set_columns.types, *set_columns.names);
	}
	return SnifferResult(detected_types, names);
}

} // namespace duckdb
