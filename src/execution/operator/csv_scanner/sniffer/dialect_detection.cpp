#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"

namespace duckdb {

constexpr idx_t CSVReaderOptions::sniff_size;

bool IsQuoteDefault(char quote) {
	if (quote == '\"' || quote == '\'' || quote == '\0') {
		return true;
	}
	return false;
}

vector<string> DialectCandidates::GetDefaultDelimiter() {
	return {",", "|", ";", "\t"};
}

vector<vector<char>> DialectCandidates::GetDefaultQuote() {
	return {{'\0'}, {'\"', '\''}, {'\"'}};
}

vector<QuoteRule> DialectCandidates::GetDefaultQuoteRule() {
	return {QuoteRule::NO_QUOTES, QuoteRule::QUOTES_OTHER, QuoteRule::QUOTES_RFC};
}

vector<vector<char>> DialectCandidates::GetDefaultEscape() {
	return {{'\0'}, {'\\'}, {'\"', '\0', '\''}};
}

vector<char> DialectCandidates::GetDefaultComment() {
	return {'#', '\0'};
}

string DialectCandidates::Print() {
	std::ostringstream search_space;

	search_space << "Delimiter Candidates: ";
	for (idx_t i = 0; i < delim_candidates.size(); i++) {
		search_space << "\'" << delim_candidates[i] << "\'";
		if (i < delim_candidates.size() - 1) {
			search_space << ", ";
		}
	}
	search_space << "\n";
	search_space << "Quote/Escape Candidates: ";
	for (uint8_t i = 0; i < static_cast<uint8_t>(quote_rule_candidates.size()); i++) {
		auto quote_candidate = quote_candidates_map[i];
		auto escape_candidate = escape_candidates_map[i];
		for (idx_t j = 0; j < quote_candidate.size(); j++) {
			for (idx_t k = 0; k < escape_candidate.size(); k++) {
				search_space << "[\'";
				if (quote_candidate[j] == '\0') {
					search_space << "(no quote)";
				} else {
					search_space << quote_candidate[j];
				}
				search_space << "\',\'";
				if (escape_candidate[k] == '\0') {
					search_space << "(no escape)";
				} else {
					search_space << escape_candidate[k];
				}
				search_space << "\']";
				if (k < escape_candidate.size() - 1) {
					search_space << ",";
				}
			}
			if (j < quote_candidate.size() - 1) {
				search_space << ",";
			}
		}
		if (i < quote_rule_candidates.size() - 1) {
			search_space << ",";
		}
	}
	search_space << "\n";

	search_space << "Comment Candidates: ";
	for (idx_t i = 0; i < comment_candidates.size(); i++) {
		search_space << "\'" << comment_candidates[i] << "\'";
		if (i < comment_candidates.size() - 1) {
			search_space << ", ";
		}
	}
	search_space << "\n";

	return search_space.str();
}

DialectCandidates::DialectCandidates(const CSVStateMachineOptions &options) {
	// assert that quotes escapes and rules have equal size
	const auto default_quote = GetDefaultQuote();
	const auto default_escape = GetDefaultEscape();
	const auto default_quote_rule = GetDefaultQuoteRule();
	const auto default_delimiter = GetDefaultDelimiter();
	const auto default_comment = GetDefaultComment();

	D_ASSERT(default_quote.size() == default_quote_rule.size() && default_quote_rule.size() == default_escape.size());
	// fill the escapes
	for (idx_t i = 0; i < default_quote_rule.size(); i++) {
		escape_candidates_map[static_cast<uint8_t>(default_quote_rule[i])] = default_escape[i];
	}

	if (options.delimiter.IsSetByUser()) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.delimiter.GetValue()};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = default_delimiter;
	}
	if (options.comment.IsSetByUser()) {
		// user provided comment character: use that as a comment
		comment_candidates = {options.comment.GetValue()};
	} else {
		// no comment provided: try standard/common comments
		comment_candidates = default_comment;
	}
	if (options.quote.IsSetByUser()) {
		// user provided quote: use that quote rule
		for (auto &quote_rule : default_quote_rule) {
			quote_candidates_map[static_cast<uint8_t>(quote_rule)] = {options.quote.GetValue()};
		}
		// also add it as an escape rule
		if (!IsQuoteDefault(options.quote.GetValue())) {
			escape_candidates_map[static_cast<uint8_t>(QuoteRule::QUOTES_RFC)].emplace_back(options.quote.GetValue());
		}
	} else {
		// no quote rule provided: use standard/common quotes
		for (idx_t i = 0; i < default_quote_rule.size(); i++) {
			quote_candidates_map[static_cast<uint8_t>(default_quote_rule[i])] = {default_quote[i]};
		}
	}
	if (options.escape.IsSetByUser()) {
		// user provided escape: use that escape rule
		if (options.escape == '\0') {
			quote_rule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quote_rule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[static_cast<uint8_t>(quote_rule_candidates[0])] = {options.escape.GetValue()};
	} else {
		// no escape provided: try standard/common escapes
		quote_rule_candidates = default_quote_rule;
	}
}

void CSVSniffer::GenerateStateMachineSearchSpace(vector<unique_ptr<ColumnCountScanner>> &column_count_scanners,
                                                 const DialectCandidates &dialect_candidates) {
	// Generate state machines for all option combinations
	NewLineIdentifier new_line_id;
	if (options.dialect_options.state_machine_options.new_line.IsSetByUser()) {
		new_line_id = options.dialect_options.state_machine_options.new_line.GetValue();
	} else {
		new_line_id = DetectNewLineDelimiter(*buffer_manager);
	}
	CSVIterator first_iterator;
	bool iterator_set = false;
	for (const auto quote_rule : dialect_candidates.quote_rule_candidates) {
		const auto &quote_candidates = dialect_candidates.quote_candidates_map.at(static_cast<uint8_t>(quote_rule));
		for (const auto &quote : quote_candidates) {
			for (const auto &delimiter : dialect_candidates.delim_candidates) {
				const auto &escape_candidates =
				    dialect_candidates.escape_candidates_map.at(static_cast<uint8_t>(quote_rule));
				for (const auto &escape : escape_candidates) {
					for (const auto &comment : dialect_candidates.comment_candidates) {
						D_ASSERT(buffer_manager);
						CSVStateMachineOptions state_machine_options(
						    delimiter, quote, escape, comment, new_line_id,
						    options.dialect_options.state_machine_options.strict_mode.GetValue());
						auto sniffing_state_machine =
						    make_shared_ptr<CSVStateMachine>(options, state_machine_options, state_machine_cache);
						if (options.dialect_options.skip_rows.IsSetByUser()) {
							if (!iterator_set) {
								first_iterator = BaseScanner::SkipCSVRows(buffer_manager, sniffing_state_machine,
								                                          options.dialect_options.skip_rows.GetValue());
								iterator_set = true;
							}
							column_count_scanners.emplace_back(make_uniq<ColumnCountScanner>(
							    buffer_manager, std::move(sniffing_state_machine), detection_error_handler,
							    CSVReaderOptions::sniff_size, first_iterator));
							continue;
						}
						column_count_scanners.emplace_back(
						    make_uniq<ColumnCountScanner>(buffer_manager, std::move(sniffing_state_machine),
						                                  detection_error_handler, CSVReaderOptions::sniff_size));
					}
				}
			}
		}
	}
}

// Returns true if a comment is acceptable
bool AreCommentsAcceptable(const ColumnCountResult &result, idx_t num_cols, bool comment_set_by_user) {
	if (comment_set_by_user) {
		return true;
	}
	// For a comment to be acceptable, we want 3/5th's the majority of unmatched in the columns
	constexpr double min_majority = 0.6;
	// detected comments, are all lines that started with a comment character.
	double detected_comments = 0;
	// If at least one comment is a full line comment
	bool has_full_line_comment = false;
	// valid comments are all lines where the number of columns does not fit our expected number of columns.
	double valid_comments = 0;
	for (idx_t i = 0; i < result.result_position; i++) {
		if (result.column_counts[i].is_comment || result.column_counts[i].is_mid_comment) {
			detected_comments++;
			if (result.column_counts[i].number_of_columns != num_cols && result.column_counts[i].is_comment) {
				has_full_line_comment = true;
				valid_comments++;
			}
			if (result.column_counts[i].number_of_columns == num_cols && result.column_counts[i].is_mid_comment) {
				valid_comments++;
			}
		}
	}
	// If we do not encounter at least one full line comment, we do not consider this comment option.
	if (valid_comments == 0 || !has_full_line_comment) {
		// this is only valid if our comment character is \0
		if (result.state_machine.state_machine_options.comment.GetValue() == '\0') {
			return true;
		}
		return false;
	}

	return valid_comments / detected_comments >= min_majority;
}

void CSVSniffer::AnalyzeDialectCandidate(unique_ptr<ColumnCountScanner> scanner, idx_t &rows_read,
                                         idx_t &best_consistent_rows, idx_t &prev_padding_count,
                                         idx_t &min_ignored_rows) {
	// The sniffed_column_counts variable keeps track of the number of columns found for each row
	auto &sniffed_column_counts = scanner->ParseChunk();
	idx_t dirty_notes = 0;
	idx_t dirty_notes_minus_comments = 0;
	if (sniffed_column_counts.error) {
		// This candidate has an error (i.e., over maximum line size or never unquoting quoted values)
		return;
	}
	idx_t consistent_rows = 0;
	idx_t num_cols = sniffed_column_counts.result_position == 0 ? 1 : sniffed_column_counts[0].number_of_columns;
	const bool ignore_errors = options.ignore_errors.GetValue();
	// If we are ignoring errors and not null_padding , we pick the most frequent number of columns as the right one
	const bool use_most_frequent_columns = ignore_errors && !options.null_padding;
	if (use_most_frequent_columns) {
		num_cols = sniffed_column_counts.GetMostFrequentColumnCount();
	}
	idx_t padding_count = 0;
	idx_t comment_rows = 0;
	idx_t ignored_rows = 0;
	const bool allow_padding = options.null_padding;
	bool first_valid = false;
	if (sniffed_column_counts.result_position > rows_read) {
		rows_read = sniffed_column_counts.result_position;
	}
	if (set_columns.IsCandidateUnacceptable(num_cols, options.null_padding, ignore_errors,
	                                        sniffed_column_counts[0].last_value_always_empty)) {
		// Not acceptable
		return;
	}
	idx_t header_idx = 0;
	for (idx_t row = 0; row < sniffed_column_counts.result_position; row++) {
		if (set_columns.IsCandidateUnacceptable(sniffed_column_counts[row].number_of_columns, options.null_padding,
		                                        ignore_errors, sniffed_column_counts[row].last_value_always_empty)) {
			// Not acceptable
			return;
		}
		if (sniffed_column_counts[row].is_comment) {
			comment_rows++;
		} else if (sniffed_column_counts[row].last_value_always_empty &&
		           sniffed_column_counts[row].number_of_columns ==
		               sniffed_column_counts[header_idx].number_of_columns + 1) {
			// we allow for the first row to miss one column IF last_value_always_empty is true
			// This is so we can sniff files that have an extra delimiter on the data part.
			// e.g., C1|C2\n1|2|\n3|4|
			consistent_rows++;
		} else if (num_cols < sniffed_column_counts[row].number_of_columns &&
		           (!options.dialect_options.skip_rows.IsSetByUser() || comment_rows > 0) &&
		           (!set_columns.IsSet() || options.null_padding) && (!first_valid || (!use_most_frequent_columns))) {
			// all rows up to this point will need padding
			if (!first_valid) {
				first_valid = true;
				sniffed_column_counts.state_machine.dialect_options.rows_until_header = row;
			}
			padding_count = 0;
			// we use the maximum amount of num_cols that we find
			num_cols = sniffed_column_counts[row].number_of_columns;
			dirty_notes = row;
			dirty_notes_minus_comments = dirty_notes - comment_rows;
			header_idx = row;
			consistent_rows = 1;
		} else if (sniffed_column_counts[row].number_of_columns == num_cols || (use_most_frequent_columns)) {
			if (!first_valid) {
				first_valid = true;
				sniffed_column_counts.state_machine.dialect_options.rows_until_header = row;
				dirty_notes = row;
			}
			if (sniffed_column_counts[row].number_of_columns != num_cols) {
				ignored_rows++;
			}
			consistent_rows++;
		} else if (num_cols >= sniffed_column_counts[row].number_of_columns) {
			// we are missing some columns, we can parse this as long as we add padding
			padding_count++;
		}
	}

	if (sniffed_column_counts.state_machine.options.dialect_options.skip_rows.IsSetByUser()) {
		sniffed_column_counts.state_machine.dialect_options.rows_until_header +=
		    sniffed_column_counts.state_machine.options.dialect_options.skip_rows.GetValue();
	}
	// Calculate the total number of consistent rows after adding padding.
	consistent_rows += padding_count;

	// Whether there are more values (rows) available that are consistent, exceeding the current best.
	const bool more_values = consistent_rows > best_consistent_rows && num_cols >= max_columns_found;

	const bool more_columns = consistent_rows == best_consistent_rows && num_cols > max_columns_found;

	// If additional padding is required when compared to the previous padding count.
	const bool require_more_padding = padding_count > prev_padding_count;

	// If less padding is now required when compared to the previous padding count.
	const bool require_less_padding = padding_count < prev_padding_count;

	// If there was only a single column before, and the new number of columns exceeds that.
	const bool single_column_before = max_columns_found < 2 && num_cols > max_columns_found * candidates.size();

	// If the number of rows is consistent with the calculated value after accounting for skipped rows and the
	// start row.
	const bool rows_consistent =
	    consistent_rows + (dirty_notes_minus_comments - options.dialect_options.skip_rows.GetValue()) + comment_rows ==
	    sniffed_column_counts.result_position - options.dialect_options.skip_rows.GetValue();
	// If there are more than one consistent row.
	const bool more_than_one_row = consistent_rows > 1;

	// If there are more than one column.
	const bool more_than_one_column = num_cols > 1;

	// If the start position is valid.
	const bool start_good = !candidates.empty() &&
	                        dirty_notes <= candidates.front()->GetStateMachine().dialect_options.skip_rows.GetValue();

	// If padding happened but it is not allowed.
	const bool invalid_padding = !allow_padding && padding_count > 0;

	const bool comments_are_acceptable = AreCommentsAcceptable(
	    sniffed_column_counts, num_cols, options.dialect_options.state_machine_options.comment.IsSetByUser());

	const bool quoted =
	    scanner->ever_quoted &&
	    sniffed_column_counts.state_machine.dialect_options.state_machine_options.quote.GetValue() != '\0';

	// For our columns to match, we either don't have them manually set, or they match in value with the sniffed value
	const bool columns_match_set =
	    num_cols == set_columns.Size() ||
	    (num_cols == set_columns.Size() + 1 && sniffed_column_counts[0].last_value_always_empty) ||
	    !set_columns.IsSet();

	// If rows are consistent and no invalid padding happens, this is the best suitable candidate if one of the
	// following is valid:
	// - There's a single column before.
	// - There are more values and no additional padding is required.
	// - There's more than one column and less padding is required.
	if (columns_match_set && (rows_consistent || (set_columns.IsSet() && ignore_errors)) &&
	    (single_column_before || ((more_values || more_columns) && !require_more_padding) ||
	     (more_than_one_column && require_less_padding) || quoted) &&
	    !invalid_padding && comments_are_acceptable) {
		if (!candidates.empty() && set_columns.IsSet() && max_columns_found == set_columns.Size() &&
		    consistent_rows <= best_consistent_rows) {
			// We have a candidate that fits our requirements better
			if (candidates.front()->ever_quoted || !scanner->ever_quoted) {
				return;
			}
		}
		auto &sniffing_state_machine = scanner->GetStateMachine();

		if (!candidates.empty() && candidates.front()->ever_quoted) {
			// Give preference to quoted boys.
			if (!scanner->ever_quoted) {
				return;
			} else {
				// Give preference to one that got escaped
				if (!scanner->ever_escaped && candidates.front()->ever_escaped) {
					return;
				}
				if (best_consistent_rows == consistent_rows && num_cols >= max_columns_found) {
					// If both have not been escaped, this might get solved later on.
					sniffing_state_machine.dialect_options.num_cols = num_cols;
					candidates.emplace_back(std::move(scanner));
					max_columns_found = num_cols;
					return;
				}
			}
		}
		if (max_columns_found == num_cols && (ignored_rows > min_ignored_rows)) {
			return;
		}
		if (max_columns_found > 1 && num_cols > max_columns_found && consistent_rows < best_consistent_rows / 2 &&
		    options.null_padding) {
			// When null_padding is true, we only give preference to a max number of columns if null padding is at least
			// 50% as consistent as the best case scenario
			return;
		}
		if (quoted && num_cols < max_columns_found) {
			for (auto &candidate : candidates) {
				if (candidate->ever_quoted) {
					return;
				}
			}
		}
		best_consistent_rows = consistent_rows;
		max_columns_found = num_cols;
		prev_padding_count = padding_count;
		min_ignored_rows = ignored_rows;

		if (options.dialect_options.skip_rows.IsSetByUser()) {
			// If skip rows is set by user, and we found dirty notes, we only accept it if either null_padding or
			// ignore_errors is set we have comments
			if (dirty_notes != 0 && !options.null_padding && !options.ignore_errors.GetValue() && comment_rows == 0) {
				return;
			}
			sniffing_state_machine.dialect_options.skip_rows = options.dialect_options.skip_rows.GetValue();
		} else if (!options.null_padding) {
			sniffing_state_machine.dialect_options.skip_rows = dirty_notes;
		}

		candidates.clear();
		sniffing_state_machine.dialect_options.num_cols = num_cols;
		lines_sniffed = sniffed_column_counts.result_position;
		candidates.emplace_back(std::move(scanner));
		return;
	}
	// If there's more than one row and column, the start is good, rows are consistent,
	// no additional padding is required, and there is no invalid padding, and there is not yet a candidate
	// with the same quote, we add this state_machine as a suitable candidate.
	if (columns_match_set && more_than_one_row && more_than_one_column && start_good && rows_consistent &&
	    !require_more_padding && !invalid_padding && num_cols == max_columns_found && comments_are_acceptable) {
		auto &sniffing_state_machine = scanner->GetStateMachine();

		if (options.dialect_options.skip_rows.IsSetByUser()) {
			// If skip rows is set by user, and we found dirty notes, we only accept it if either null_padding or
			// ignore_errors is set
			if (dirty_notes != 0 && !options.null_padding && !options.ignore_errors.GetValue()) {
				return;
			}
			sniffing_state_machine.dialect_options.skip_rows = options.dialect_options.skip_rows.GetValue();
		} else if (!options.null_padding) {
			sniffing_state_machine.dialect_options.skip_rows = dirty_notes;
		}
		sniffing_state_machine.dialect_options.num_cols = num_cols;
		lines_sniffed = sniffed_column_counts.result_position;
		candidates.emplace_back(std::move(scanner));
	}
}

bool CSVSniffer::RefineCandidateNextChunk(ColumnCountScanner &candidate) const {
	auto &sniffed_column_counts = candidate.ParseChunk();
	for (idx_t i = 0; i < sniffed_column_counts.result_position; i++) {
		if (set_columns.IsSet()) {
			return !set_columns.IsCandidateUnacceptable(sniffed_column_counts[i].number_of_columns,
			                                            options.null_padding, options.ignore_errors.GetValue(),
			                                            sniffed_column_counts[i].last_value_always_empty);
		}
		if (max_columns_found != sniffed_column_counts[i].number_of_columns &&
		    (!options.null_padding && !options.ignore_errors.GetValue() && !sniffed_column_counts[i].is_comment)) {
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
	if (candidates.size() == 1 || candidates[0]->FinishedFile()) {
		// Only one candidate nothing to refine or all candidates already checked
		return;
	}

	for (idx_t i = 1; i <= options.sample_size_chunks; i++) {
		vector<unique_ptr<ColumnCountScanner>> successful_candidates;
		bool done = candidates.empty();
		for (auto &cur_candidate : candidates) {
			const bool finished_file = cur_candidate->FinishedFile();
			if (successful_candidates.empty()) {
				lines_sniffed += cur_candidate->GetResult().result_position;
			}
			if (finished_file || i == options.sample_size_chunks) {
				// we finished the file or our chunk sample successfully
				if (!cur_candidate->GetResult().error) {
					successful_candidates.push_back(std::move(cur_candidate));
				}
				done = true;
				continue;
			}
			if (RefineCandidateNextChunk(*cur_candidate) && !cur_candidate->GetResult().error) {
				successful_candidates.push_back(std::move(cur_candidate));
			}
		}
		candidates = std::move(successful_candidates);
		if (done) {
			break;
		}
	}
	// If we have multiple candidates with quotes set, we will give the preference to ones
	// that have actually quoted values, otherwise we will choose quotes = \0
	vector<unique_ptr<ColumnCountScanner>> successful_candidates = std::move(candidates);
	if (!successful_candidates.empty()) {
		for (idx_t i = 0; i < successful_candidates.size(); i++) {
			unique_ptr<ColumnCountScanner> cc_best_candidate = std::move(successful_candidates[i]);
			if (cc_best_candidate->state_machine->state_machine_options.quote != '\0' &&
			    cc_best_candidate->ever_quoted) {
				// If we have multiple candidates with the same quote, but different escapes
				for (idx_t j = i + 1; j < successful_candidates.size(); j++) {
					// we give preference if it has the same character between escape and quote
					if (successful_candidates[j]->state_machine->state_machine_options.escape ==
					    successful_candidates[j]->state_machine->state_machine_options.quote) {
						cc_best_candidate = std::move(successful_candidates[j]);
						break;
					}
				}
				candidates.clear();
				candidates.push_back(std::move(cc_best_candidate));
				return;
			}
			candidates.push_back(std::move(cc_best_candidate));
		}
	}
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
	if (carriage_return) {
		return NewLineIdentifier::SINGLE_R;
	}
	return NewLineIdentifier::SINGLE_N;
}

// Dialect Detection consists of five steps:
// 1. Generate a search space of all possible dialects
// 2. Generate a state machine for each dialect
// 3. Analyze the first chunk of the file and find the best dialect candidates
// 4. Analyze the remaining chunks of the file and find the best dialect candidate
void CSVSniffer::DetectDialect() {
	// Variables for Dialect Detection
	DialectCandidates dialect_candidates(options.dialect_options.state_machine_options);
	// Number of rows read
	idx_t rows_read = 0;
	// Best Number of consistent rows (i.e., presenting all columns)
	idx_t best_consistent_rows = 0;
	// If padding was necessary (i.e., rows are missing some columns, how many)
	idx_t prev_padding_count = 0;
	// Min number of ignores rows
	idx_t best_ignored_rows = 0;
	// Vector of CSV State Machines
	vector<unique_ptr<ColumnCountScanner>> csv_state_machines;
	// Step 1: Generate state machines
	GenerateStateMachineSearchSpace(csv_state_machines, dialect_candidates);
	// Step 2: Analyze all candidates on the first chunk
	for (auto &state_machine : csv_state_machines) {
		AnalyzeDialectCandidate(std::move(state_machine), rows_read, best_consistent_rows, prev_padding_count,
		                        best_ignored_rows);
	}
	// Step 3: Loop over candidates and find if they can still produce good results for the remaining chunks
	RefineCandidates();

	// if no dialect candidate was found, we throw an exception
	if (candidates.empty()) {
		auto error = CSVError::SniffingError(options, dialect_candidates.Print());
		error_handler->Error(error, true);
	}
}
} // namespace duckdb
