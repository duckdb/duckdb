#include "duckdb/execution/operator/persistent/csv_scanner/buffered_csv_reader.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "utf8proc.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_buffer_manager.hpp"
#include <cmath>

namespace duckdb {

static bool StartsWithNumericDate(string &separator, const string &value) {
	auto begin = value.c_str();
	auto end = begin + value.size();

	//	StrpTimeFormat::Parse will skip whitespace, so we can too
	auto field1 = std::find_if_not(begin, end, StringUtil::CharacterIsSpace);
	if (field1 == end) {
		return false;
	}

	//	first numeric field must start immediately
	if (!StringUtil::CharacterIsDigit(*field1)) {
		return false;
	}
	auto literal1 = std::find_if_not(field1, end, StringUtil::CharacterIsDigit);
	if (literal1 == end) {
		return false;
	}

	//	second numeric field must exist
	auto field2 = std::find_if(literal1, end, StringUtil::CharacterIsDigit);
	if (field2 == end) {
		return false;
	}
	auto literal2 = std::find_if_not(field2, end, StringUtil::CharacterIsDigit);
	if (literal2 == end) {
		return false;
	}

	//	third numeric field must exist
	auto field3 = std::find_if(literal2, end, StringUtil::CharacterIsDigit);
	if (field3 == end) {
		return false;
	}

	//	second literal must match first
	if (((field3 - literal2) != (field2 - literal1)) || strncmp(literal1, literal2, (field2 - literal1)) != 0) {
		return false;
	}

	//	copy the literal as the separator, escaping percent signs
	separator.clear();
	while (literal1 < field2) {
		const auto literal_char = *literal1++;
		if (literal_char == '%') {
			separator.push_back(literal_char);
		}
		separator.push_back(literal_char);
	}

	return true;
}

string GenerateDateFormat(const string &separator, const char *format_template) {
	string format_specifier = format_template;
	auto amount_of_dashes = std::count(format_specifier.begin(), format_specifier.end(), '-');
	if (!amount_of_dashes) {
		return format_specifier;
	}
	string result;
	result.reserve(format_specifier.size() - amount_of_dashes + (amount_of_dashes * separator.size()));
	for (auto &character : format_specifier) {
		if (character == '-') {
			result += separator;
		} else {
			result += character;
		}
	}
	return result;
}

// Helper function to generate column names
static string GenerateColumnName(const idx_t total_cols, const idx_t col_number, const string &prefix = "column") {
	int max_digits = NumericHelper::UnsignedLength(total_cols - 1);
	int digits = NumericHelper::UnsignedLength(col_number);
	string leading_zeros = string(max_digits - digits, '0');
	string value = to_string(col_number);
	return string(prefix + leading_zeros + value);
}

// Helper function for UTF-8 aware space trimming
static string TrimWhitespace(const string &col_name) {
	utf8proc_int32_t codepoint;
	auto str = reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str());
	idx_t size = col_name.size();
	// Find the first character that is not left trimmed
	idx_t begin = 0;
	while (begin < size) {
		auto bytes = utf8proc_iterate(str + begin, size - begin, &codepoint);
		D_ASSERT(bytes > 0);
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			break;
		}
		begin += bytes;
	}

	// Find the last character that is not right trimmed
	idx_t end;
	end = begin;
	for (auto next = begin; next < col_name.size();) {
		auto bytes = utf8proc_iterate(str + next, size - next, &codepoint);
		D_ASSERT(bytes > 0);
		next += bytes;
		if (utf8proc_category(codepoint) != UTF8PROC_CATEGORY_ZS) {
			end = next;
		}
	}

	// return the trimmed string
	return col_name.substr(begin, end - begin);
}

static string NormalizeColumnName(const string &col_name) {
	// normalize UTF8 characters to NFKD
	auto nfkd = utf8proc_NFKD(reinterpret_cast<const utf8proc_uint8_t *>(col_name.c_str()), col_name.size());
	const string col_name_nfkd = string(const_char_ptr_cast(nfkd), strlen(const_char_ptr_cast(nfkd)));
	free(nfkd);

	// only keep ASCII characters 0-9 a-z A-Z and replace spaces with regular whitespace
	string col_name_ascii = "";
	for (idx_t i = 0; i < col_name_nfkd.size(); i++) {
		if (col_name_nfkd[i] == '_' || (col_name_nfkd[i] >= '0' && col_name_nfkd[i] <= '9') ||
		    (col_name_nfkd[i] >= 'A' && col_name_nfkd[i] <= 'Z') ||
		    (col_name_nfkd[i] >= 'a' && col_name_nfkd[i] <= 'z')) {
			col_name_ascii += col_name_nfkd[i];
		} else if (StringUtil::CharacterIsSpace(col_name_nfkd[i])) {
			col_name_ascii += " ";
		}
	}

	// trim whitespace and replace remaining whitespace by _
	string col_name_trimmed = TrimWhitespace(col_name_ascii);
	string col_name_cleaned = "";
	bool in_whitespace = false;
	for (idx_t i = 0; i < col_name_trimmed.size(); i++) {
		if (col_name_trimmed[i] == ' ') {
			if (!in_whitespace) {
				col_name_cleaned += "_";
				in_whitespace = true;
			}
		} else {
			col_name_cleaned += col_name_trimmed[i];
			in_whitespace = false;
		}
	}

	// don't leave string empty; if not empty, make lowercase
	if (col_name_cleaned.empty()) {
		col_name_cleaned = "_";
	} else {
		col_name_cleaned = StringUtil::Lower(col_name_cleaned);
	}

	// prepend _ if name starts with a digit or is a reserved keyword
	if (KeywordHelper::IsKeyword(col_name_cleaned) || (col_name_cleaned[0] >= '0' && col_name_cleaned[0] <= '9')) {
		col_name_cleaned = "_" + col_name_cleaned;
	}
	return col_name_cleaned;
}

bool BufferedCSVReader::JumpToNextSample() {
	// get bytes contained in the previously read chunk
	idx_t remaining_bytes_in_buffer = buffer_size - start;
	bytes_in_chunk -= remaining_bytes_in_buffer;
	if (remaining_bytes_in_buffer == 0) {
		return false;
	}

	// assess if it makes sense to jump, based on size of the first chunk relative to size of the entire file
	if (sample_chunk_idx == 0) {
		idx_t bytes_first_chunk = bytes_in_chunk;
		double chunks_fit = (file_handle->FileSize() / (double)bytes_first_chunk);
		jumping_samples = chunks_fit >= options.sample_chunks;

		// jump back to the beginning
		JumpToBeginning(options.skip_rows, options.header);
		sample_chunk_idx++;
		return true;
	}

	if (end_of_file_reached || sample_chunk_idx >= options.sample_chunks) {
		return false;
	}

	// if we deal with any other sources than plaintext files, jumping_samples can be tricky. In that case
	// we just read x continuous chunks from the stream TODO: make jumps possible for zipfiles.
	if (!file_handle->OnDiskFile() || !jumping_samples) {
		sample_chunk_idx++;
		return true;
	}

	// update average bytes per line
	double bytes_per_line = bytes_in_chunk / (double)options.sample_chunk_size;
	bytes_per_line_avg = ((bytes_per_line_avg * (sample_chunk_idx)) + bytes_per_line) / (sample_chunk_idx + 1);

	// if none of the previous conditions were met, we can jump
	idx_t partition_size = (idx_t)round(file_handle->FileSize() / (double)options.sample_chunks);

	// calculate offset to end of the current partition
	int64_t offset = partition_size - bytes_in_chunk - remaining_bytes_in_buffer;
	auto current_pos = file_handle->SeekPosition();

	if (current_pos + offset < file_handle->FileSize()) {
		// set position in stream and clear failure bits
		file_handle->Seek(current_pos + offset);

		// estimate linenr
		linenr += (idx_t)round((offset + remaining_bytes_in_buffer) / bytes_per_line_avg);
		linenr_estimated = true;
	} else {
		// seek backwards from the end in last chunk and hope to catch the end of the file
		// TODO: actually it would be good to make sure that the end of file is being reached, because
		// messy end-lines are quite common. For this case, however, we first need a skip_end detection anyways.
		file_handle->Seek(file_handle->FileSize() - bytes_in_chunk);

		// estimate linenr
		linenr = (idx_t)round((file_handle->FileSize() - bytes_in_chunk) / bytes_per_line_avg);
		linenr_estimated = true;
	}

	// reset buffers and parse chunk
	ResetBuffer();

	// seek beginning of next line
	// FIXME: if this jump ends up in a quoted linebreak, we will have a problem
	string read_line = file_handle->ReadLine();
	linenr++;

	sample_chunk_idx++;

	return true;
}

void CSVSniffer::AnalyzeDialectCandidate(CSVStateMachine &state_machine, idx_t prev_column_count) {
	vector<idx_t> sniffed_column_counts(STANDARD_VECTOR_SIZE);
	state_machine.SniffDialect(sniffed_column_counts);

	idx_t start_row = options.skip_rows;
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
			start_row = row + options.skip_rows;
			consistent_rows = 1;

		} else if (num_cols >= sniffed_column_counts[row]) {
			// we are missing some columns, we can parse this as long as we add padding
			padding_count++;
		}
	}

	if (num_cols < prev_column_count) {
		// Early return if we have less columns than the previous chunk run
		return;
	}

	// some logic
	consistent_rows += padding_count;
	bool more_values = (consistent_rows > best_consistent_rows && num_cols >= best_num_cols);
	bool require_more_padding = padding_count > prev_padding_count;
	bool require_less_padding = padding_count < prev_padding_count;
	bool single_column_before = best_num_cols < 2 && num_cols > best_num_cols;
	bool rows_consistent = start_row + consistent_rows - options.skip_rows == sniffed_column_counts.size();
	bool more_than_one_row = (consistent_rows > 1);
	bool more_than_one_column = (num_cols > 1);
	bool start_good = !candidates.empty() && (start_row <= candidates.front().state->configuration.start_row);
	bool invalid_padding = !allow_padding && padding_count > 0;

	if (!requested_types.empty() && requested_types.size() != num_cols && !invalid_padding) {
		return;
	} else if (rows_consistent &&
	           (single_column_before || (more_values && !require_more_padding) ||
	            (more_than_one_column && require_less_padding)) &&
	           !invalid_padding) {
		best_consistent_rows = consistent_rows;
		best_num_cols = num_cols;
		prev_padding_count = padding_count;
		state_machine.configuration.start_row = start_row;
		candidates.clear();
		candidates.emplace_back(&state_machine, num_cols);
	} else if (more_than_one_row && more_than_one_column && start_good && rows_consistent && !require_more_padding &&
	           !invalid_padding) {
		bool same_quote_is_candidate = false;
		for (auto &candidate : candidates) {
			if (state_machine.configuration.quote == candidate.state->configuration.quote) {
				same_quote_is_candidate = true;
			}
		}
		if (!same_quote_is_candidate) {
			state_machine.configuration.start_row = start_row;
			candidates.emplace_back(&state_machine, num_cols);
		}
	}
}

void CSVSniffer::ResetStats() {
	// Reset stats
	rows_read = 0;
	best_consistent_rows = 0;
	prev_padding_count = 0;
	best_num_cols = 0;
}

SnifferResult CSVSniffer::SniffCSV() {
}

void CSVSniffer::GenerateCandidateDetectionSearchSpace() {
	if (options.has_delimiter) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.delimiter};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = {',', '|', ';', '\t'};
	}
	if (options.has_quote) {
		// user provided quote: use that quote rule
		quote_candidates_map = {{options.quote}, {options.quote}, {options.quote}};
	} else {
		// no quote rule provided: use standard/common quotes
		quote_candidates_map = {{'\"'}, {'\"', '\''}, {'\0'}};
	}
	if (options.has_escape) {
		// user provided escape: use that escape rule
		if (options.escape == '\0') {
			quoterule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quoterule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[static_cast<uint8_t>(quoterule_candidates[0])] = {options.escape};
	} else {
		// no escape provided: try standard/common escapes
		quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	}
}

void CSVSniffer::GenerateStateMachineSearchSpace() {
	// Generate state machines for all option combinations
	for (auto quoterule : quoterule_candidates) {
		const auto &quote_candidates = quote_candidates_map[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delim : delim_candidates) {
				const auto &escape_candidates = escape_candidates_map[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					CSVStateMachineConfiguration configuration(delim, quote, escape, options.new_line);
					D_ASSERT(buffer_manager);
					csv_state_machines.emplace_back(configuration, buffer_manager);
				}
			}
		}
	}
}

vector<CSVReaderOptions> CSVSniffer::ProduceDialectResults() {
	vector<CSVReaderOptions> result;
	for (auto &candidate : candidates) {
		auto option = options;
		option.quote = candidate.state->configuration.quote;
		option.escape = candidate.state->configuration.escape;
		option.delimiter = candidate.state->configuration.field_separator;
		option.num_cols = candidate.max_num_columns;
		option.new_line = candidate.state->configuration.record_separator;
		option.skip_rows = candidate.state->configuration.start_row;
		result.emplace_back(option);
	}
	return result;
}

void CSVSniffer::RefineCandidates() {
	auto cur_best_num_cols = best_num_cols;
	for (idx_t i = 1; i < options.sample_chunks; i++) {
		if (candidates.size() <= 1) {
			// no candidates or we only have one candidate left: stop
			return;
		}
		bool finished_file = candidates[0].state->csv_buffer_iterator.Finished();
		if (finished_file) {
			// we finished the file: stop
			return;
		}
		ResetStats();
		auto cur_candidates = std::move(candidates);
		cur_best_num_cols = std::max(best_num_cols, cur_best_num_cols);
		for (auto &cur_candidate : cur_candidates) {
			AnalyzeDialectCandidate(*cur_candidate.state, cur_best_num_cols);
		}
	}
}

// Dialect Detection consists of five steps:
// 1. Generate a search space of all possible dialects
// 2. Generate a state machine for each dialect
// 3. Analyze the first chunk of the file and find the best dialect candidates
// 4. Analyze the remaining chunks of the file and find the best dialect candidate
// 5. Return the converted dialect options
vector<CSVReaderOptions> CSVSniffer::DetectDialect() {
	// Step 1: Generate search space
	GenerateCandidateDetectionSearchSpace();
	// Step 2: Generate state machines
	GenerateStateMachineSearchSpace();
	// Step 3: Analyze all candidates on the first chunk
	for (auto &state_machine : csv_state_machines) {
		AnalyzeDialectCandidate(state_machine);
	}
	// Step 4: Loop over candidates and find if they can still produce good results for the remaining chunks
	RefineCandidates();
	// if no dialect candidate was found, we throw an exception
	if (candidates.empty()) {
		throw InvalidInputException(
		    "Error in file \"%s\": CSV options could not be auto-detected. Consider setting parser options manually.",
		    options.file_path);
	}
	// Step 5: Produce the result
	return ProduceDialectResults();
}

void BufferedCSVReader::DetectCandidateTypes(const vector<LogicalType> &type_candidates,
                                             const map<LogicalTypeId, vector<const char *>> &format_template_candidates,
                                             const vector<CSVReaderOptions> &info_candidates,
                                             CSVReaderOptions &original_options, idx_t best_num_cols,
                                             vector<vector<LogicalType>> &best_sql_types_candidates,
                                             std::map<LogicalTypeId, vector<string>> &best_format_candidates,
                                             DataChunk &best_header_row) {
	CSVReaderOptions best_options;
	idx_t min_varchar_cols = best_num_cols + 1;

	// check which info candidate leads to minimum amount of non-varchar columns...
	for (const auto &t : format_template_candidates) {
		best_format_candidates[t.first].clear();
	}
	for (auto &info_candidate : info_candidates) {
		options = info_candidate;
		vector<vector<LogicalType>> info_sql_types_candidates(options.num_cols, type_candidates);
		std::map<LogicalTypeId, bool> has_format_candidates;
		std::map<LogicalTypeId, vector<string>> format_candidates;
		for (const auto &t : format_template_candidates) {
			has_format_candidates[t.first] = false;
			format_candidates[t.first].clear();
		}

		// set all return_types to VARCHAR so we can do datatype detection based on VARCHAR values
		return_types.clear();
		return_types.assign(options.num_cols, LogicalType::VARCHAR);

		// jump to beginning and skip potential header
		JumpToBeginning(options.skip_rows, true);
		DataChunk header_row;
		header_row.Initialize(allocator, return_types);
		parse_chunk.Copy(header_row);

		if (header_row.size() == 0) {
			continue;
		}

		// init parse chunk and read csv with info candidate
		InitParseChunk(return_types.size());
		if (!TryParseCSV(ParserMode::SNIFFING_DATATYPES)) {
			continue;
		}
		for (idx_t row_idx = 0; row_idx <= parse_chunk.size(); row_idx++) {
			bool is_header_row = row_idx == 0;
			idx_t row = row_idx - 1;
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				auto &col_type_candidates = info_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					// try cast from string to sql_type
					Value dummy_val;
					if (is_header_row) {
						VerifyUTF8(col, 0, header_row, -int64_t(parse_chunk.size()));
						dummy_val = header_row.GetValue(col, 0);
					} else {
						VerifyUTF8(col, row, parse_chunk);
						dummy_val = parse_chunk.GetValue(col, row);
					}
					// try formatting for date types if the user did not specify one and it starts with numeric values.
					string separator;
					if (has_format_candidates.count(sql_type.id()) && !original_options.has_format[sql_type.id()] &&
					    !dummy_val.IsNull() && StartsWithNumericDate(separator, StringValue::Get(dummy_val))) {
						// generate date format candidates the first time through
						auto &type_format_candidates = format_candidates[sql_type.id()];
						const auto had_format_candidates = has_format_candidates[sql_type.id()];
						if (!has_format_candidates[sql_type.id()]) {
							has_format_candidates[sql_type.id()] = true;
							// order by preference
							auto entry = format_template_candidates.find(sql_type.id());
							if (entry != format_template_candidates.end()) {
								const auto &format_template_list = entry->second;
								for (const auto &t : format_template_list) {
									const auto format_string = GenerateDateFormat(separator, t);
									// don't parse ISO 8601
									if (format_string.find("%Y-%m-%d") == string::npos) {
										type_format_candidates.emplace_back(format_string);
									}
								}
							}
							//	initialise the first candidate
							options.has_format[sql_type.id()] = true;
							//	all formats are constructed to be valid
							SetDateFormat(type_format_candidates.back(), sql_type.id());
						}
						// check all formats and keep the first one that works
						StrpTimeFormat::ParseResult result;
						auto save_format_candidates = type_format_candidates;
						while (!type_format_candidates.empty()) {
							//	avoid using exceptions for flow control...
							auto &current_format = options.date_format[sql_type.id()];
							if (current_format.Parse(StringValue::Get(dummy_val), result)) {
								break;
							}
							//	doesn't work - move to the next one
							type_format_candidates.pop_back();
							options.has_format[sql_type.id()] = (!type_format_candidates.empty());
							if (!type_format_candidates.empty()) {
								SetDateFormat(type_format_candidates.back(), sql_type.id());
							}
						}
						//	if none match, then this is not a value of type sql_type,
						if (type_format_candidates.empty()) {
							//	so restore the candidates that did work.
							//	or throw them out if they were generated by this value.
							if (had_format_candidates) {
								type_format_candidates.swap(save_format_candidates);
								if (!type_format_candidates.empty()) {
									SetDateFormat(type_format_candidates.back(), sql_type.id());
								}
							} else {
								has_format_candidates[sql_type.id()] = false;
							}
						}
					}
					// try cast from string to sql_type
					if (TryCastValue(dummy_val, sql_type)) {
						break;
					} else {
						col_type_candidates.pop_back();
					}
				}
			}
			// reset type detection, because first row could be header,
			// but only do it if csv has more than one line (including header)
			if (parse_chunk.size() > 0 && is_header_row) {
				info_sql_types_candidates = vector<vector<LogicalType>>(options.num_cols, type_candidates);
				for (auto &f : format_candidates) {
					f.second.clear();
				}
				for (auto &h : has_format_candidates) {
					h.second = false;
				}
			}
		}

		idx_t varchar_cols = 0;
		for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
			auto &col_type_candidates = info_sql_types_candidates[col];
			// check number of varchar columns
			const auto &col_type = col_type_candidates.back();
			if (col_type == LogicalType::VARCHAR) {
				varchar_cols++;
			}
		}

		// it's good if the dialect creates more non-varchar columns, but only if we sacrifice < 30% of best_num_cols.
		if (varchar_cols < min_varchar_cols && parse_chunk.ColumnCount() > (best_num_cols * 0.7)) {
			// we have a new best_options candidate
			best_options = info_candidate;
			min_varchar_cols = varchar_cols;
			best_sql_types_candidates = info_sql_types_candidates;
			best_format_candidates = format_candidates;
			best_header_row.Destroy();
			auto header_row_types = header_row.GetTypes();
			best_header_row.Initialize(allocator, header_row_types);
			header_row.Copy(best_header_row);
		}
	}

	options = best_options;
	for (const auto &best : best_format_candidates) {
		if (!best.second.empty()) {
			SetDateFormat(best.second.back(), best.first);
		}
	}
}

void BufferedCSVReader::DetectHeader(const vector<vector<LogicalType>> &best_sql_types_candidates,
                                     const DataChunk &best_header_row) {
	// information for header detection
	bool first_row_consistent = true;
	bool first_row_nulls = false;

	// check if header row is all null and/or consistent with detected column data types
	first_row_nulls = true;
	for (idx_t col = 0; col < best_sql_types_candidates.size(); col++) {
		auto dummy_val = best_header_row.GetValue(col, 0);
		if (!dummy_val.IsNull()) {
			first_row_nulls = false;
		}

		// try cast to sql_type of column
		const auto &sql_type = best_sql_types_candidates[col].back();
		if (!TryCastValue(dummy_val, sql_type)) {
			first_row_consistent = false;
		}
	}

	// update parser info, and read, generate & set col_names based on previous findings
	if (((!first_row_consistent || first_row_nulls) && !options.has_header) || (options.has_header && options.header)) {
		options.header = true;
		case_insensitive_map_t<idx_t> name_collision_count;
		// get header names from CSV
		for (idx_t col = 0; col < options.num_cols; col++) {
			const auto &val = best_header_row.GetValue(col, 0);
			string col_name = val.ToString();

			// generate name if field is empty
			if (col_name.empty() || val.IsNull()) {
				col_name = GenerateColumnName(options.num_cols, col);
			}

			// normalize names or at least trim whitespace
			if (options.normalize_names) {
				col_name = NormalizeColumnName(col_name);
			} else {
				col_name = TrimWhitespace(col_name);
			}

			// avoid duplicate header names
			const string col_name_raw = col_name;
			while (name_collision_count.find(col_name) != name_collision_count.end()) {
				name_collision_count[col_name] += 1;
				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
			}

			names.push_back(col_name);
			name_collision_count[col_name] = 0;
		}

	} else {
		options.header = false;
		for (idx_t col = 0; col < options.num_cols; col++) {
			string column_name = GenerateColumnName(options.num_cols, col);
			names.push_back(column_name);
		}
	}
	for (idx_t i = 0; i < MinValue<idx_t>(names.size(), options.name_list.size()); i++) {
		names[i] = options.name_list[i];
	}
}

vector<LogicalType> BufferedCSVReader::RefineTypeDetection(const vector<LogicalType> &type_candidates,
                                                           const vector<LogicalType> &requested_types,
                                                           vector<vector<LogicalType>> &best_sql_types_candidates,
                                                           map<LogicalTypeId, vector<string>> &best_format_candidates) {
	// for the type refine we set the SQL types to VARCHAR for all columns
	return_types.clear();
	return_types.assign(options.num_cols, LogicalType::VARCHAR);

	vector<LogicalType> detected_types;

	// if data types were provided, exit here if number of columns does not match
	if (!requested_types.empty()) {
		if (requested_types.size() != options.num_cols) {
			throw InvalidInputException(
			    "Error while determining column types: found %lld columns but expected %d. (%s)", options.num_cols,
			    requested_types.size(), options.ToString());
		} else {
			detected_types = requested_types;
		}
	} else if (options.all_varchar) {
		// return all types varchar
		detected_types = return_types;
	} else {
		// jump through the rest of the file and continue to refine the sql type guess
		while (JumpToNextSample()) {
			InitParseChunk(return_types.size());
			// if jump ends up a bad line, we just skip this chunk
			if (!TryParseCSV(ParserMode::SNIFFING_DATATYPES)) {
				continue;
			}
			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
				vector<LogicalType> &col_type_candidates = best_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					//	narrow down the date formats
					if (best_format_candidates.count(sql_type.id())) {
						auto &best_type_format_candidates = best_format_candidates[sql_type.id()];
						auto save_format_candidates = best_type_format_candidates;
						while (!best_type_format_candidates.empty()) {
							if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
								break;
							}
							//	doesn't work - move to the next one
							best_type_format_candidates.pop_back();
							options.has_format[sql_type.id()] = (!best_type_format_candidates.empty());
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
							}
						}
						//	if none match, then this is not a column of type sql_type,
						if (best_type_format_candidates.empty()) {
							//	so restore the candidates that did work.
							best_type_format_candidates.swap(save_format_candidates);
							if (!best_type_format_candidates.empty()) {
								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
							}
						}
					}

					if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
						break;
					} else {
						col_type_candidates.pop_back();
					}
				}
			}
		}

		// set sql types
		for (auto &best_sql_types_candidate : best_sql_types_candidates) {
			LogicalType d_type = best_sql_types_candidate.back();
			if (best_sql_types_candidate.size() == type_candidates.size()) {
				d_type = LogicalType::VARCHAR;
			}
			detected_types.push_back(d_type);
		}
	}

	return detected_types;
}

// vector<LogicalType> BufferedCSVReader::SniffCSV(const vector<LogicalType> &requested_types) {
//	// Check if any type is BLOB
//	for (auto &type : requested_types) {
//		// auto-detect for blobs not supported: there may be invalid UTF-8 in the file
//		if (type.id() == LogicalTypeId::BLOB) {
//			return requested_types;
//		}
//	}
//
//	if (options.skip_rows_set) {
//		// Skip rows if they are set
//		SkipRowsAndReadHeader(options.skip_rows, false);
//	}
//
//	// #######
//	// ### dialect detection
//	// #######
//
//
//	CSVReaderOptions original_options = options;
//	vector<CSVReaderOptions> info_candidates;
//	idx_t best_num_cols = 0;
//
//	info_candidates = sniffer.DetectDialect();
//	for (auto &candidate : info_candidates) {
//		best_num_cols = std::max(best_num_cols, candidate.num_cols);
//	}
//
//	// FIXME: hack to make this work with both buffers. Ideally this whole code must work with the buffer manager
//	file_handle->Reset();
//	if (options.skip_rows_set) {
//		// Skip rows if they are set
//		SkipRowsAndReadHeader(options.skip_rows, false);
//	}
//	ReadBuffer(start, start);
//	// #######
//	// ### type detection (initial)
//	// #######
//
//	// format template candidates, ordered by descending specificity (~ from high to low)
//	std::map<LogicalTypeId, vector<const char *>> format_template_candidates = {
//	    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
//	    {LogicalTypeId::TIMESTAMP,
//	     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
//	      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S"}},
//	};
//	vector<vector<LogicalType>> best_sql_types_candidates;
//	map<LogicalTypeId, vector<string>> best_format_candidates;
//	DataChunk best_header_row;
//	DetectCandidateTypes(options.auto_type_candidates, format_template_candidates, info_candidates, original_options,
//	                     best_num_cols, best_sql_types_candidates, best_format_candidates, best_header_row);
//
//	if (best_format_candidates.empty() || best_header_row.size() == 0) {
//		throw InvalidInputException(
//		    "Error in file \"%s\": CSV options could not be auto-detected. Consider setting parser options manually.",
//		    original_options.file_path);
//	}
//
//	// #######
//	// ### header detection
//	// #######
//	options.num_cols = best_sql_types_candidates.size();
//	DetectHeader(best_sql_types_candidates, best_header_row);
//	if (!options.sql_type_list.empty()) {
//		// user-defined types were supplied for certain columns
//		// override the types
//		if (!options.sql_types_per_column.empty()) {
//			// types supplied as name -> value map
//			idx_t found = 0;
//			for (idx_t i = 0; i < names.size(); i++) {
//				auto it = options.sql_types_per_column.find(names[i]);
//				if (it != options.sql_types_per_column.end()) {
//					best_sql_types_candidates[i] = {options.sql_type_list[it->second]};
//					found++;
//					continue;
//				}
//			}
//			if (!options.file_options.union_by_name && found < options.sql_types_per_column.size()) {
//				string exception = ColumnTypesError(options.sql_types_per_column, names);
//				if (!exception.empty()) {
//					throw BinderException(exception);
//				}
//			}
//		} else {
//			// types supplied as list
//			if (names.size() < options.sql_type_list.size()) {
//				throw BinderException("read_csv: %d types were provided, but CSV file only has %d columns",
//				                      options.sql_type_list.size(), names.size());
//			}
//			for (idx_t i = 0; i < options.sql_type_list.size(); i++) {
//				best_sql_types_candidates[i] = {options.sql_type_list[i]};
//			}
//		}
//	}
//
//	// #######
//	// ### type detection (refining)
//	// #######
//	return RefineTypeDetection(options.auto_type_candidates, requested_types, best_sql_types_candidates,
//	                           best_format_candidates);
//}
} // namespace duckdb
