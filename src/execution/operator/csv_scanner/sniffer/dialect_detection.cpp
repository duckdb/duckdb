#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

bool IsQuoteDefault(char quote) {
	if (quote == '\"' || quote == '\'' || quote == '\0') {
		return true;
	}
	return false;
}

void CSVSniffer::GenerateCandidateDetectionSearchSpace(vector<char> &delim_candidates,
                                                       vector<QuoteRule> &quoterule_candidates,
                                                       unordered_map<uint8_t, vector<char>> &quote_candidates_map,
                                                       unordered_map<uint8_t, vector<char>> &escape_candidates_map) {
	if (options.dialect_options.state_machine_options.delimiter.IsSetByUser()) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.dialect_options.state_machine_options.delimiter.GetValue()};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = {',', '|', ';', '\t'};
	}
	if (options.dialect_options.state_machine_options.quote.IsSetByUser()) {
		// user provided quote: use that quote rule
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {
		    options.dialect_options.state_machine_options.quote.GetValue()};
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {
		    options.dialect_options.state_machine_options.quote.GetValue()};
		quote_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {
		    options.dialect_options.state_machine_options.quote.GetValue()};
		// also add it as a escape rule
		if (!IsQuoteDefault(options.dialect_options.state_machine_options.quote.GetValue())) {
			escape_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC].emplace_back(
			    options.dialect_options.state_machine_options.quote.GetValue());
		}
	} else {
		// no quote rule provided: use standard/common quotes
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {'\"'};
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {'\"', '\''};
		quote_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {'\0'};
	}
	if (options.dialect_options.state_machine_options.escape.IsSetByUser()) {
		// user provided escape: use that escape rule
		if (options.dialect_options.state_machine_options.escape == '\0') {
			quoterule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quoterule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[(uint8_t)quoterule_candidates[0]] = {
		    options.dialect_options.state_machine_options.escape.GetValue()};
	} else {
		// no escape provided: try standard/common escapes
		quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	}
}

void CSVSniffer::GenerateStateMachineSearchSpace(vector<unique_ptr<ColumnCountScanner>> &column_count_scanners,
                                                 const vector<char> &delimiter_candidates,
                                                 const vector<QuoteRule> &quoterule_candidates,
                                                 const unordered_map<uint8_t, vector<char>> &quote_candidates_map,
                                                 const unordered_map<uint8_t, vector<char>> &escape_candidates_map) {
	// Generate state machines for all option combinations
	NewLineIdentifier new_line_id;
	if (options.dialect_options.state_machine_options.new_line.IsSetByUser()) {
		new_line_id = options.dialect_options.state_machine_options.new_line.GetValue();
	} else {
		new_line_id = DetectNewLineDelimiter(*buffer_manager);
	}
	for (const auto quoterule : quoterule_candidates) {
		const auto &quote_candidates = quote_candidates_map.at((uint8_t)quoterule);
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : delimiter_candidates) {
				const auto &escape_candidates = escape_candidates_map.at((uint8_t)quoterule);
				for (const auto &escape : escape_candidates) {
					D_ASSERT(buffer_manager);
					CSVStateMachineOptions state_machine_options(delimiter, quote, escape, new_line_id);
					auto sniffing_state_machine =
					    make_uniq<CSVStateMachine>(options, state_machine_options, state_machine_cache);
					column_count_scanners.emplace_back(make_uniq<ColumnCountScanner>(
					    buffer_manager, std::move(sniffing_state_machine), detection_error_handler));
				}
			}
		}
	}
}

void CSVSniffer::AnalyzeDialectCandidate(unique_ptr<ColumnCountScanner> scanner, idx_t &rows_read,
                                         idx_t &best_consistent_rows, idx_t &prev_padding_count) {
	// The sniffed_column_counts variable keeps track of the number of columns found for each row
	auto &sniffed_column_counts = scanner->ParseChunk();
	if (sniffed_column_counts.error) {
		// This candidate has an error (i.e., over maximum line size or never unquoting quoted values)
		return;
	}
	idx_t start_row = options.dialect_options.skip_rows.GetValue();
	idx_t consistent_rows = 0;
	idx_t num_cols = sniffed_column_counts.result_position == 0 ? 1 : sniffed_column_counts[start_row];
	idx_t padding_count = 0;
	bool allow_padding = options.null_padding;
	if (sniffed_column_counts.result_position > rows_read) {
		rows_read = sniffed_column_counts.result_position;
	}
	if (set_columns.IsCandidateUnacceptable(num_cols, options.null_padding, options.ignore_errors.GetValue(),
	                                        sniffed_column_counts.last_value_always_empty)) {
		// Not acceptable
		return;
	}
	for (idx_t row = start_row; row < sniffed_column_counts.result_position; row++) {
		if (set_columns.IsCandidateUnacceptable(sniffed_column_counts[row], options.null_padding,
		                                        options.ignore_errors.GetValue(),
		                                        sniffed_column_counts.last_value_always_empty)) {
			// Not acceptable
			return;
		}
		if (sniffed_column_counts[row] == num_cols || (options.ignore_errors.GetValue() && !options.null_padding)) {
			consistent_rows++;
		} else if (num_cols < sniffed_column_counts[row] && !options.dialect_options.skip_rows.IsSetByUser() &&
		           (!set_columns.IsSet() || options.null_padding)) {
			// all rows up to this point will need padding
			padding_count = 0;
			// we use the maximum amount of num_cols that we find
			num_cols = sniffed_column_counts[row];
			start_row = row;
			consistent_rows = 1;

		} else if (num_cols >= sniffed_column_counts[row]) {
			// we are missing some columns, we can parse this as long as we add padding
			padding_count++;
		}
	}

	// Calculate the total number of consistent rows after adding padding.
	consistent_rows += padding_count;

	// Whether there are more values (rows) available that are consistent, exceeding the current best.
	bool more_values = (consistent_rows > best_consistent_rows && num_cols >= max_columns_found);

	// If additional padding is required when compared to the previous padding count.
	bool require_more_padding = padding_count > prev_padding_count;

	// If less padding is now required when compared to the previous padding count.
	bool require_less_padding = padding_count < prev_padding_count;

	// If there was only a single column before, and the new number of columns exceeds that.
	bool single_column_before = max_columns_found < 2 && num_cols > max_columns_found * candidates.size();

	// If the number of rows is consistent with the calculated value after accounting for skipped rows and the
	// start row.
	bool rows_consistent = consistent_rows + (start_row - options.dialect_options.skip_rows.GetValue()) ==
	                       sniffed_column_counts.result_position - options.dialect_options.skip_rows.GetValue();
	// If there are more than one consistent row.
	bool more_than_one_row = (consistent_rows > 1);

	// If there are more than one column.
	bool more_than_one_column = (num_cols > 1);

	// If the start position is valid.
	bool start_good = !candidates.empty() &&
	                  (start_row <= candidates.front()->GetStateMachine().dialect_options.skip_rows.GetValue());

	// If padding happened but it is not allowed.
	bool invalid_padding = !allow_padding && padding_count > 0;

	// If rows are consistent and no invalid padding happens, this is the best suitable candidate if one of the
	// following is valid:
	// - There's a single column before.
	// - There are more values and no additional padding is required.
	// - There's more than one column and less padding is required.
	if (rows_consistent &&
	    (single_column_before || (more_values && !require_more_padding) ||
	     (more_than_one_column && require_less_padding)) &&
	    !invalid_padding) {
		if (!candidates.empty() && set_columns.IsSet() && max_columns_found == candidates.size()) {
			// We have a candidate that fits our requirements better
			return;
		}
		auto &sniffing_state_machine = scanner->GetStateMachine();

		if (!candidates.empty() && candidates.front()->ever_quoted && !scanner->ever_quoted) {
			// Give preference to quoted boys.
			return;
		}

		best_consistent_rows = consistent_rows;
		max_columns_found = num_cols;
		prev_padding_count = padding_count;
		if (!options.null_padding && !options.ignore_errors.GetValue()) {
			sniffing_state_machine.dialect_options.skip_rows = start_row;
		} else {
			sniffing_state_machine.dialect_options.skip_rows = options.dialect_options.skip_rows.GetValue();
		}
		candidates.clear();
		sniffing_state_machine.dialect_options.num_cols = num_cols;
		candidates.emplace_back(std::move(scanner));
		return;
	}
	// If there's more than one row and column, the start is good, rows are consistent,
	// no additional padding is required, and there is no invalid padding, and there is not yet a candidate
	// with the same quote, we add this state_machine as a suitable candidate.
	if (more_than_one_row && more_than_one_column && start_good && rows_consistent && !require_more_padding &&
	    !invalid_padding && num_cols == max_columns_found) {
		auto &sniffing_state_machine = scanner->GetStateMachine();

		bool same_quote_is_candidate = false;
		for (auto &candidate : candidates) {
			if (sniffing_state_machine.dialect_options.state_machine_options.quote ==
			    candidate->GetStateMachine().dialect_options.state_machine_options.quote) {
				same_quote_is_candidate = true;
			}
		}
		if (!same_quote_is_candidate) {
			if (!options.null_padding && !options.ignore_errors.GetValue()) {
				sniffing_state_machine.dialect_options.skip_rows = start_row;
			} else {
				sniffing_state_machine.dialect_options.skip_rows = options.dialect_options.skip_rows.GetValue();
			}
			sniffing_state_machine.dialect_options.num_cols = num_cols;
			candidates.emplace_back(std::move(scanner));
		}
	}
}

bool CSVSniffer::RefineCandidateNextChunk(ColumnCountScanner &candidate) {
	auto &sniffed_column_counts = candidate.ParseChunk();
	for (idx_t i = 0; i < sniffed_column_counts.result_position; i++) {
		if (set_columns.IsSet()) {
			return !set_columns.IsCandidateUnacceptable(sniffed_column_counts[i], options.null_padding,
			                                            options.ignore_errors.GetValue(),
			                                            sniffed_column_counts.last_value_always_empty);
		} else {
			if (max_columns_found != sniffed_column_counts[i] &&
			    (!options.null_padding && !options.ignore_errors.GetValue())) {
				return false;
			}
		}
	}
	return true;
}

void CSVSniffer::RefineCandidates() {
	// It's very frequent that more than one dialect can parse a csv file, hence here we run one state machine
	// fully on the whole sample dataset, when/if it fails we go to the next one.
	if (candidates.empty()) {
		// No candidates to refine
		return;
	}
	if (candidates.size() == 1 || candidates[0]->FinishedFile()) {
		// Only one candidate nothing to refine or all candidates already checked
		return;
	}
	vector<unique_ptr<ColumnCountScanner>> successful_candidates;
	for (auto &cur_candidate : candidates) {
		for (idx_t i = 1; i <= options.sample_size_chunks; i++) {
			bool finished_file = cur_candidate->FinishedFile();
			if (finished_file || i == options.sample_size_chunks) {
				// we finished the file or our chunk sample successfully
				successful_candidates.push_back(std::move(cur_candidate));
				break;
			}
			if (!RefineCandidateNextChunk(*cur_candidate) || cur_candidate->GetResult().error) {
				// This candidate failed, move to the next one
				break;
			}
		}
	}
	// If we have multiple candidates with quotes set, we will give the preference to ones
	// that have actually quoted values, otherwise we will choose quotes = \0
	candidates.clear();
	if (!successful_candidates.empty()) {
		unique_ptr<ColumnCountScanner> cc_best_candidate;
		for (idx_t i = 0; i < successful_candidates.size(); i++) {
			cc_best_candidate = std::move(successful_candidates[i]);
			if (cc_best_candidate->state_machine->state_machine_options.quote != '\0' &&
			    cc_best_candidate->ever_quoted) {
				candidates.clear();
				candidates.push_back(std::move(cc_best_candidate));
				return;
			}
			candidates.push_back(std::move(cc_best_candidate));
		}
		return;
	}
	return;
}

NewLineIdentifier CSVSniffer::DetectNewLineDelimiter(CSVBufferManager &buffer_manager) {
	// Get first buffer
	auto buffer = buffer_manager.GetBuffer(0);
	auto buffer_ptr = buffer->Ptr();
	bool carriage_return = false;
	bool n = false;
	for (idx_t i = 0; i < buffer->actual_size; i++) {
		if (buffer_ptr[i] == '\r') {
			carriage_return = true;
		} else if (buffer_ptr[i] == '\n') {
			n = true;
			break;
		} else if (carriage_return) {
			break;
		}
	}
	if (carriage_return && n) {
		return NewLineIdentifier::CARRY_ON;
	}
	return NewLineIdentifier::SINGLE;
}

// Dialect Detection consists of five steps:
// 1. Generate a search space of all possible dialects
// 2. Generate a state machine for each dialect
// 3. Analyze the first chunk of the file and find the best dialect candidates
// 4. Analyze the remaining chunks of the file and find the best dialect candidate
void CSVSniffer::DetectDialect() {
	// Variables for Dialect Detection
	// Candidates for the delimiter
	vector<char> delim_candidates;
	// Quote-Rule Candidates
	vector<QuoteRule> quoterule_candidates;
	// Candidates for the quote option
	unordered_map<uint8_t, vector<char>> quote_candidates_map;
	// Candidates for the escape option
	unordered_map<uint8_t, vector<char>> escape_candidates_map;
	escape_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {'\"', '\'', '\0'};
	escape_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {'\\'};
	escape_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {'\0'};
	// Number of rows read
	idx_t rows_read = 0;
	// Best Number of consistent rows (i.e., presenting all columns)
	idx_t best_consistent_rows = 0;
	// If padding was necessary (i.e., rows are missing some columns, how many)
	idx_t prev_padding_count = 0;
	// Vector of CSV State Machines
	vector<unique_ptr<ColumnCountScanner>> csv_state_machines;

	// Step 1: Generate search space
	GenerateCandidateDetectionSearchSpace(delim_candidates, quoterule_candidates, quote_candidates_map,
	                                      escape_candidates_map);
	// Step 2: Generate state machines
	GenerateStateMachineSearchSpace(csv_state_machines, delim_candidates, quoterule_candidates, quote_candidates_map,
	                                escape_candidates_map);
	// Step 3: Analyze all candidates on the first chunk
	for (auto &state_machine : csv_state_machines) {
		state_machine->Reset();
		AnalyzeDialectCandidate(std::move(state_machine), rows_read, best_consistent_rows, prev_padding_count);
	}
	// Step 4: Loop over candidates and find if they can still produce good results for the remaining chunks
	RefineCandidates();

	// if no dialect candidate was found, we throw an exception
	if (candidates.empty()) {
		auto error = CSVError::SniffingError(options.file_path);
		error_handler->Error(error);
	}
}
} // namespace duckdb
