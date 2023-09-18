#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

struct SniffDialect {
	inline static void Initialize(CSVStateMachine &machine) {
		machine.state = CSVState::STANDARD;
		machine.previous_state = CSVState::STANDARD;
		machine.pre_previous_state = CSVState::STANDARD;
		machine.cur_rows = 0;
		machine.column_count = 1;
	}

	inline static bool Process(CSVStateMachine &machine, vector<idx_t> &sniffed_column_counts, char current_char,
	                           idx_t current_pos) {

		D_ASSERT(sniffed_column_counts.size() == machine.options.sample_chunk_size);

		if (machine.state == CSVState::INVALID) {
			sniffed_column_counts.clear();
			return true;
		}
		machine.pre_previous_state = machine.previous_state;
		machine.previous_state = machine.state;

		machine.state = static_cast<CSVState>(
		    machine.transition_array[static_cast<uint8_t>(machine.state)][static_cast<uint8_t>(current_char)]);

		bool carriage_return = machine.previous_state == CSVState::CARRIAGE_RETURN;
		machine.column_count += machine.previous_state == CSVState::DELIMITER;
		sniffed_column_counts[machine.cur_rows] = machine.column_count;
		machine.cur_rows +=
		    machine.previous_state == CSVState::RECORD_SEPARATOR && machine.state != CSVState::EMPTY_LINE;
		machine.column_count -= (machine.column_count - 1) * (machine.previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		machine.cur_rows += machine.state != CSVState::RECORD_SEPARATOR && carriage_return;
		machine.column_count -=
		    (machine.column_count - 1) * (machine.state != CSVState::RECORD_SEPARATOR && carriage_return);

		// Identify what is our line separator
		machine.carry_on_separator =
		    (machine.state == CSVState::RECORD_SEPARATOR && carriage_return) || machine.carry_on_separator;
		machine.single_record_separator = ((machine.state != CSVState::RECORD_SEPARATOR && carriage_return) ||
		                                   (machine.state == CSVState::RECORD_SEPARATOR && !carriage_return)) ||
		                                  machine.single_record_separator;
		if (machine.cur_rows >= machine.options.sample_chunk_size) {
			// We sniffed enough rows
			return true;
		}
		return false;
	}
	inline static void Finalize(CSVStateMachine &machine, vector<idx_t> &sniffed_column_counts) {
		if (machine.state == CSVState::INVALID) {
			return;
		}
		if (machine.cur_rows < machine.options.sample_chunk_size && machine.state == CSVState::DELIMITER) {
			sniffed_column_counts[machine.cur_rows] = ++machine.column_count;
		}
		if (machine.cur_rows < machine.options.sample_chunk_size && machine.state != CSVState::EMPTY_LINE) {
			sniffed_column_counts[machine.cur_rows++] = machine.column_count;
		}
		NewLineIdentifier suggested_newline;
		if (machine.carry_on_separator) {
			if (machine.single_record_separator) {
				suggested_newline = NewLineIdentifier::MIX;
			} else {
				suggested_newline = NewLineIdentifier::CARRY_ON;
			}
		} else {
			suggested_newline = NewLineIdentifier::SINGLE;
		}
		if (machine.options.dialect_options.new_line == NewLineIdentifier::NOT_SET) {
			machine.dialect_options.new_line = suggested_newline;
		} else {
			if (machine.options.dialect_options.new_line != suggested_newline) {
				// Invalidate this whole detection
				machine.cur_rows = 0;
			}
		}
		sniffed_column_counts.erase(sniffed_column_counts.begin() + machine.cur_rows, sniffed_column_counts.end());
	}
};

void CSVSniffer::GenerateCandidateDetectionSearchSpace(vector<char> &delim_candidates,
                                                       vector<QuoteRule> &quoterule_candidates,
                                                       unordered_map<uint8_t, vector<char>> &quote_candidates_map,
                                                       unordered_map<uint8_t, vector<char>> &escape_candidates_map) {
	if (options.has_delimiter) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.dialect_options.state_machine_options.delimiter};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = {',', '|', ';', '\t'};
	}
	if (options.has_quote) {
		// user provided quote: use that quote rule
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {options.dialect_options.state_machine_options.quote};
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {options.dialect_options.state_machine_options.quote};
		quote_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {options.dialect_options.state_machine_options.quote};
	} else {
		// no quote rule provided: use standard/common quotes
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {'\"'};
		quote_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {'\"', '\''};
		quote_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {'\0'};
	}
	if (options.has_escape) {
		// user provided escape: use that escape rule
		if (options.dialect_options.state_machine_options.escape == '\0') {
			quoterule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quoterule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[(uint8_t)quoterule_candidates[0]] = {
		    options.dialect_options.state_machine_options.escape};
	} else {
		// no escape provided: try standard/common escapes
		quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	}
}

void CSVSniffer::GenerateStateMachineSearchSpace(vector<unique_ptr<CSVStateMachine>> &csv_state_machines,
                                                 const vector<char> &delimiter_candidates,
                                                 const vector<QuoteRule> &quoterule_candidates,
                                                 const unordered_map<uint8_t, vector<char>> &quote_candidates_map,
                                                 const unordered_map<uint8_t, vector<char>> &escape_candidates_map) {
	// Generate state machines for all option combinations
	for (const auto quoterule : quoterule_candidates) {
		const auto &quote_candidates = quote_candidates_map.at((uint8_t)quoterule);
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : delimiter_candidates) {
				const auto &escape_candidates = escape_candidates_map.at((uint8_t)quoterule);
				for (const auto &escape : escape_candidates) {
					D_ASSERT(buffer_manager);
					CSVStateMachineOptions state_machine_options(delimiter, quote, escape);
					csv_state_machines.emplace_back(make_uniq<CSVStateMachine>(options, state_machine_options,
					                                                           buffer_manager, state_machine_cache));
				}
			}
		}
	}
}

void CSVSniffer::AnalyzeDialectCandidate(unique_ptr<CSVStateMachine> state_machine, idx_t &rows_read,
                                         idx_t &best_consistent_rows, idx_t &prev_padding_count) {
	// The sniffed_column_counts variable keeps track of the number of columns found for each row
	vector<idx_t> sniffed_column_counts(options.sample_chunk_size);

	state_machine->csv_buffer_iterator.Process<SniffDialect>(*state_machine, sniffed_column_counts);
	idx_t start_row = options.dialect_options.skip_rows;
	idx_t consistent_rows = 0;
	idx_t num_cols = sniffed_column_counts.empty() ? 0 : sniffed_column_counts[0];
	idx_t padding_count = 0;
	bool allow_padding = options.null_padding;
	if (sniffed_column_counts.size() > rows_read) {
		rows_read = sniffed_column_counts.size();
	}
	for (idx_t row = 0; row < sniffed_column_counts.size(); row++) {
		if (sniffed_column_counts[row] == num_cols) {
			consistent_rows++;
		} else if (num_cols < sniffed_column_counts[row] && !options.skip_rows_set) {
			// all rows up to this point will need padding
			padding_count = 0;
			// we use the maximum amount of num_cols that we find
			num_cols = sniffed_column_counts[row];
			start_row = row + options.dialect_options.skip_rows;
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
	bool single_column_before = max_columns_found < 2 && num_cols > max_columns_found;

	// If the number of rows is consistent with the calculated value after accounting for skipped rows and the
	// start row.
	bool rows_consistent =
	    start_row + consistent_rows - options.dialect_options.skip_rows == sniffed_column_counts.size();

	// If there are more than one consistent row.
	bool more_than_one_row = (consistent_rows > 1);

	// If there are more than one column.
	bool more_than_one_column = (num_cols > 1);

	// If the start position is valid.
	bool start_good = !candidates.empty() && (start_row <= candidates.front()->start_row);

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
		best_consistent_rows = consistent_rows;
		max_columns_found = num_cols;
		prev_padding_count = padding_count;
		state_machine->start_row = start_row;
		candidates.clear();
		state_machine->dialect_options.num_cols = num_cols;
		candidates.emplace_back(std::move(state_machine));
		return;
	}
	// If there's more than one row and column, the start is good, rows are consistent,
	// no additional padding is required, and there is no invalid padding, and there is not yet a candidate
	// with the same quote, we add this state_machine as a suitable candidate.
	if (more_than_one_row && more_than_one_column && start_good && rows_consistent && !require_more_padding &&
	    !invalid_padding) {
		bool same_quote_is_candidate = false;
		for (auto &candidate : candidates) {
			if (state_machine->dialect_options.state_machine_options.quote ==
			    candidate->dialect_options.state_machine_options.quote) {
				same_quote_is_candidate = true;
			}
		}
		if (!same_quote_is_candidate) {
			state_machine->start_row = start_row;
			state_machine->dialect_options.num_cols = num_cols;
			candidates.emplace_back(std::move(state_machine));
		}
	}
}

bool CSVSniffer::RefineCandidateNextChunk(CSVStateMachine &candidate) {
	vector<idx_t> sniffed_column_counts(options.sample_chunk_size);
	candidate.csv_buffer_iterator.Process<SniffDialect>(candidate, sniffed_column_counts);
	bool allow_padding = options.null_padding;

	for (idx_t row = 0; row < sniffed_column_counts.size(); row++) {
		if (max_columns_found != sniffed_column_counts[row] && !allow_padding) {
			return false;
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
	if (candidates.size() == 1 || candidates[0]->csv_buffer_iterator.Finished()) {
		// Only one candidate nothing to refine or all candidates already checked
		return;
	}
	for (auto &cur_candidate : candidates) {
		for (idx_t i = 1; i <= options.sample_chunks; i++) {
			bool finished_file = cur_candidate->csv_buffer_iterator.Finished();
			if (finished_file || i == options.sample_chunks) {
				// we finished the file or our chunk sample successfully: stop
				auto successful_candidate = std::move(cur_candidate);
				candidates.clear();
				candidates.emplace_back(std::move(successful_candidate));
				return;
			}
			cur_candidate->cur_rows = 0;
			cur_candidate->column_count = 1;
			if (!RefineCandidateNextChunk(*cur_candidate)) {
				// This candidate failed, move to the next one
				break;
			}
		}
	}
	candidates.clear();
	return;
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
	escape_candidates_map[(uint8_t)QuoteRule::QUOTES_RFC] = {'\0', '\"', '\''};
	escape_candidates_map[(uint8_t)QuoteRule::QUOTES_OTHER] = {'\\'};
	escape_candidates_map[(uint8_t)QuoteRule::NO_QUOTES] = {'\0'};
	// Number of rows read
	idx_t rows_read = 0;
	// Best Number of consistent rows (i.e., presenting all columns)
	idx_t best_consistent_rows = 0;
	// If padding was necessary (i.e., rows are missing some columns, how many)
	idx_t prev_padding_count = 0;
	// Vector of CSV State Machines
	vector<unique_ptr<CSVStateMachine>> csv_state_machines;

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
		throw InvalidInputException(
		    "Error in file \"%s\": CSV options could not be auto-detected. Consider setting parser options manually.",
		    options.file_path);
	}
}
} // namespace duckdb
