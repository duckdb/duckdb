#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/gzip_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_from_file.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <queue>

using namespace duckdb;
using namespace std;

static char is_newline(char c) {
	return c == '\n' || c == '\r';
}

// Helper function to generate column names
static string GenerateColumnName(const idx_t total_cols, const idx_t col_number, const string prefix = "column") {
	uint8_t max_digits = total_cols > 10 ? (int)log10((double)total_cols - 1) + 1 : 1;
	uint8_t digits = col_number > 10 ? (int)log10((double)col_number) + 1 : 1;
	string leading_zeros = string("0", max_digits - digits);
	string value = std::to_string(col_number);
	return string(prefix + leading_zeros + value);
}

static string GetLineNumberStr(idx_t linenr, bool linenr_estimated) {
	string estimated = (linenr_estimated ? string(" (estimated)") : string(""));
	return std::to_string(linenr) + estimated;
}

TextSearchShiftArray::TextSearchShiftArray() {
}

TextSearchShiftArray::TextSearchShiftArray(string search_term) : length(search_term.size()) {
	if (length > 255) {
		throw Exception("Size of delimiter/quote/escape in CSV reader is limited to 255 bytes");
	}
	// initialize the shifts array
	shifts = unique_ptr<uint8_t[]>(new uint8_t[length * 255]);
	memset(shifts.get(), 0, length * 255 * sizeof(uint8_t));
	// iterate over each of the characters in the array
	for (idx_t main_idx = 0; main_idx < length; main_idx++) {
		uint8_t current_char = (uint8_t)search_term[main_idx];
		// now move over all the remaining positions
		for (idx_t i = main_idx; i < length; i++) {
			bool is_match = true;
			// check if the prefix matches at this position
			// if it does, we move to this position after encountering the current character
			for (idx_t j = 0; j < main_idx; j++) {
				if (search_term[i - main_idx + j] != search_term[j]) {
					is_match = false;
				}
			}
			if (!is_match) {
				continue;
			}
			shifts[i * 255 + current_char] = main_idx + 1;
		}
	}
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, CopyInfo &info, vector<SQLType> requested_types)
    : info(info), buffer_size(0), position(0), start(0) {
	source = OpenCSV(context, info);
	Initialize(requested_types);
}

BufferedCSVReader::BufferedCSVReader(CopyInfo &info, vector<SQLType> requested_types, unique_ptr<istream> ssource)
    : info(info), source(move(ssource)), buffer_size(0), position(0), start(0) {
	Initialize(requested_types);
}

void BufferedCSVReader::Initialize(vector<SQLType> requested_types) {
	if (info.auto_detect) {
		sql_types = SniffCSV(requested_types);
	} else {
		sql_types = requested_types;
	}

	PrepareComplexParser();
	InitParseChunk(sql_types.size());
	SkipHeader();
}

void BufferedCSVReader::PrepareComplexParser() {
	delimiter_search = TextSearchShiftArray(info.delimiter);
	escape_search = TextSearchShiftArray(info.escape);
	quote_search = TextSearchShiftArray(info.quote);
}

unique_ptr<istream> BufferedCSVReader::OpenCSV(ClientContext &context, CopyInfo &info) {
	if (!FileSystem::GetFileSystem(context).FileExists(info.file_path)) {
		throw IOException("File \"%s\" not found", info.file_path.c_str());
	}
	unique_ptr<istream> result;
	// decide based on the extension which stream to use
	if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
		result = make_unique<GzipStream>(info.file_path);
		plain_file_source = false;
	} else {
		auto csv_local = make_unique<ifstream>();
		csv_local->open(info.file_path);
		result = move(csv_local);

		// determine filesize
		plain_file_source = true;
		result->seekg(0, result->end);
		file_size = (idx_t)result->tellg();
		result->clear();
		result->seekg(0, result->beg);
	}
	return result;
}

void BufferedCSVReader::SkipHeader() {
	for (idx_t i = 0; i < info.skip_rows; i++) {
		// ignore skip rows
		string read_line;
		getline(*source, read_line);
		linenr++;
	}

	if (info.header) {
		// ignore the first line as a header line
		string read_line;
		getline(*source, read_line);
		linenr++;
	}
}

void BufferedCSVReader::ResetBuffer() {
	buffer.reset();
	buffer_size = 0;
	position = 0;
	start = 0;
	cached_buffers.clear();
}

void BufferedCSVReader::ResetStream() {
	if (!plain_file_source && StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
		// seeking to the beginning appears to not be supported in all compiler/os-scenarios,
		// so we have to create a new stream source here for now
		source = make_unique<GzipStream>(info.file_path);
	} else {
		source->clear();
		source->seekg(0, source->beg);
	}
	linenr = 0;
	linenr_estimated = false;
	bytes_per_line_avg = 0;
	sample_chunk_idx = 0;
	jumping_samples = false;
}

void BufferedCSVReader::ResetParseChunk() {
	bytes_in_chunk = 0;
	parse_chunk.Reset();
}

void BufferedCSVReader::InitParseChunk(idx_t num_cols) {
	// adapt not null info
	if (info.force_not_null.size() != num_cols) {
		info.force_not_null.resize(num_cols, false);
	}

	// destroy previous chunk
	parse_chunk.Destroy();

	// initialize the parse_chunk with a set of VARCHAR types
	vector<TypeId> varchar_types(num_cols, TypeId::VARCHAR);
	parse_chunk.Initialize(varchar_types);
}

void BufferedCSVReader::JumpToBeginning() {
	ResetBuffer();
	ResetStream();
	ResetParseChunk();
	SkipHeader();
}

bool BufferedCSVReader::JumpToNextSample() {
	if (source->eof() || sample_chunk_idx >= MAX_SAMPLE_CHUNKS) {
		return false;
	}

	// update average bytes per line
	double bytes_per_line = bytes_in_chunk / (double)SAMPLE_CHUNK_SIZE;
	bytes_per_line_avg = ((bytes_per_line_avg * sample_chunk_idx) + bytes_per_line) / (sample_chunk_idx + 1);

	// assess if it makes sense to jump, based on size of the first chunk relative to size of the entire file
	if (sample_chunk_idx == 0) {
		idx_t bytes_first_chunk = bytes_in_chunk;
		double chunks_fit = (file_size / (double)bytes_first_chunk);
		jumping_samples = chunks_fit >= (MAX_SAMPLE_CHUNKS - 1);
	}

	// if we deal with any other sources than plaintext files, jumping_samples can be tricky. In that case
	// we just read x continuous chunks from the stream TODO: make jumps possible for zipfiles.
	if (!plain_file_source || !jumping_samples) {
		sample_chunk_idx++;
		ResetParseChunk();
		return true;
	}

	// adjust the value of bytes_in_chunk, based on current state of the buffer
	idx_t remaining_bytes_in_buffer = buffer_size - start;
	bytes_in_chunk -= remaining_bytes_in_buffer;

	// if none of the previous conditions were met, we can jump
	idx_t partition_size = (idx_t)round(file_size / (double)MAX_SAMPLE_CHUNKS);

	// calculate offset to end of the current partition
	int64_t offset = partition_size - bytes_in_chunk - remaining_bytes_in_buffer;
	idx_t current_pos = (idx_t)source->tellg();

	if (current_pos + offset < file_size) {
		// set position in stream and clear failure bits
		source->clear();
		source->seekg(offset, source->cur);

		// estimate linenr
		linenr += (idx_t)round((offset + remaining_bytes_in_buffer) / bytes_per_line_avg);
		linenr_estimated = true;
	} else {
		// seek backwards from the end in last chunk and hope to catch the end of the file
		// TODO: actually it would be good to make sure that the end of file is being reached, because
		// messy end-lines are quite common. For this case, however, we first need a skip_end detection anyways.
		source->seekg(-bytes_in_chunk, source->end);

		// estimate linenr
		linenr = (idx_t)round((file_size - bytes_in_chunk) / bytes_per_line_avg);
		linenr_estimated = true;
	}

	// reset buffers and internal positions
	ResetBuffer();
	ResetParseChunk();

	// seek beginning of next line
	// FIXME: if this jump ends up in a quoted linebreak, we will have a problem
	string read_line;
	getline(*source, read_line);
	linenr++;

	sample_chunk_idx++;

	return true;
}

vector<SQLType> BufferedCSVReader::SniffCSV(vector<SQLType> requested_types) {
	// TODO: sniff for uncommon (UTF-8) delimiter variants in first lines and add them to the list
	const vector<string> delim_candidates = {",", "|", ";", "\t"};
	const vector<QuoteRule> quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER,
	                                                QuoteRule::NO_QUOTES};
	// quote candiates depend on quote rule
	const vector<vector<string>> quote_candidates_map = {{"\""}, {"\"", "'"}, {""}};
	// escape candiates also depend on quote rule.
	// Note: RFC-conform escapes are handled automatically, and without quotes no escape char is required
	const vector<vector<string>> escape_candidates_map = {{""}, {"\\"}, {""}};

	vector<CopyInfo> info_candidates;
	idx_t best_consistent_rows = 0;
	idx_t best_num_cols = 0;

	// if requested_types were provided, use them already in dialect detection
	// TODO: currently they only serve to solve the edge case of trailing empty delimiters,
	// however, they could be used to solve additional ambigious scenarios.
	sql_types = requested_types;
	// TODO: add a flag to indicate that no option actually worked and default will be used (RFC-4180)
	for (QuoteRule quoterule : quoterule_candidates) {
		vector<string> quote_candidates = quote_candidates_map[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delim : delim_candidates) {
				vector<string> escape_candidates = escape_candidates_map[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					CopyInfo sniff_info = info;
					sniff_info.delimiter = delim;
					sniff_info.quote = quote;
					sniff_info.escape = escape;
					info = sniff_info;
					PrepareComplexParser();

					ResetBuffer();
					ResetStream();
					sniffed_column_counts.clear();
					try {
						ParseCSV(ParserMode::SNIFFING_DIALECT);
					} catch (const ParserException &e) {
						continue;
					}

					idx_t start_row = 0;
					idx_t consistent_rows = 0;
					idx_t num_cols = 0;

					for (idx_t row = 0; row < sniffed_column_counts.size(); row++) {
						if (sniffed_column_counts[row] == num_cols) {
							consistent_rows++;
						} else {
							num_cols = sniffed_column_counts[row];
							start_row = row;
							consistent_rows = 1;
						}
					}

					// some logic
					bool more_values = (consistent_rows > best_consistent_rows && num_cols >= best_num_cols);
					bool single_column_before = best_num_cols < 2 && num_cols > best_num_cols;
					bool rows_conistent = start_row + consistent_rows == sniffed_column_counts.size();
					bool more_than_one_row = (consistent_rows > 1);
					bool more_than_one_column = (num_cols > 1);
					bool start_good = info_candidates.size() > 0 && (start_row <= info_candidates.front().skip_rows);

					if ((more_values || single_column_before) && rows_conistent) {
						sniff_info.skip_rows = start_row;
						sniff_info.num_cols = num_cols;
						best_consistent_rows = consistent_rows;
						best_num_cols = num_cols;

						info_candidates.clear();
						info_candidates.push_back(sniff_info);
					} else if (more_than_one_row && more_than_one_column && start_good && rows_conistent) {
						bool same_quote_is_candidate = false;
						for (auto &info_candidate : info_candidates) {
							if (quote.compare(info_candidate.quote) == 0) {
								same_quote_is_candidate = true;
							}
						}
						if (!same_quote_is_candidate) {
							sniff_info.skip_rows = start_row;
							sniff_info.num_cols = num_cols;
							info_candidates.push_back(sniff_info);
						}
					}
				}
			}
		}
	}

	// then, file was most likely empty and we can do no more
	if (info_candidates.size() < 1) {
		return requested_types;
	}

	// type candidates, ordered by descending specificity (~ from high to low)
	vector<SQLType> type_candidates = {SQLType::VARCHAR, SQLType::TIMESTAMP, SQLType::DATE,
	                                   SQLType::TIME,    SQLType::DOUBLE,    /*SQLType::FLOAT,*/ SQLType::BIGINT,
	                                   SQLType::INTEGER, SQLType::SMALLINT,  /*SQLType::TINYINT,*/ SQLType::BOOLEAN,
	                                   SQLType::SQLNULL};

	// check which info candiate leads to minimum amount of non-varchar columns...
	CopyInfo best_info;
	idx_t min_varchar_cols = best_num_cols + 1;
	vector<vector<SQLType>> best_sql_types_candidates;
	for (auto &info_candidate : info_candidates) {
		info = info_candidate;
		vector<vector<SQLType>> info_sql_types_candidates(info.num_cols, type_candidates);

		// set all sql_types to VARCHAR so we can do datatype detection based on VARCHAR values
		sql_types.clear();
		sql_types.assign(info.num_cols, SQLType::VARCHAR);
		InitParseChunk(sql_types.size());

		// detect types in first chunk
		JumpToBeginning();
		ParseCSV(ParserMode::SNIFFING_DATATYPES);
		for (idx_t row = 0; row < parse_chunk.size(); row++) {
			for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
				vector<SQLType> &col_type_candidates = info_sql_types_candidates[col];
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					// try cast from string to sql_type
					auto dummy_val = parse_chunk.GetValue(col, row);
					try {
						dummy_val.CastAs(SQLType::VARCHAR, sql_type, true);
						break;
					} catch (const Exception &e) {
						col_type_candidates.pop_back();
					}
				}
			}
			// reset type detection for second row, because first row could be header,
			// but only do it if csv has more than one line
			if (parse_chunk.size() > 1 && row == 0) {
				info_sql_types_candidates = vector<vector<SQLType>>(info.num_cols, type_candidates);
			}
		}

		// check number of varchar columns
		idx_t varchar_cols = 0;
		for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
			const auto &col_type = info_sql_types_candidates[col].back();
			if (col_type == SQLType::VARCHAR) {
				varchar_cols++;
			}
		}

		// it's good if the dialect creates more non-varchar columns, but only if we sacrifice < 40% of best_num_cols.
		if (varchar_cols < min_varchar_cols && parse_chunk.column_count() > (best_num_cols * 0.7)) {
			// we have a new best_info candidate
			best_info = info_candidate;
			min_varchar_cols = varchar_cols;
			best_sql_types_candidates = info_sql_types_candidates;
		}
	}

	info = best_info;

	// if data types were provided, exit here if number of columns does not match
	// TODO: we could think about postponing this to see if the csv happens to contain a superset of requested columns
	if (requested_types.size() > 0 && requested_types.size() != info.num_cols) {
		throw ParserException("Error while determining column types: found %lld columns but expected %d", info.num_cols,
		                      requested_types.size());
	}

	// sql_types and parse_chunk have to be in line with new info
	sql_types.clear();
	sql_types.assign(info.num_cols, SQLType::VARCHAR);
	InitParseChunk(sql_types.size());

	// jump through the rest of the file and continue to refine the sql type guess
	while (JumpToNextSample()) {
		// if jump ends up a bad line, we just skip this chunk
		try {
			ParseCSV(ParserMode::SNIFFING_DATATYPES);
		} catch (const ParserException &e) {
			continue;
		}
		for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
			vector<SQLType> &col_type_candidates = best_sql_types_candidates[col];
			while (col_type_candidates.size() > 1) {
				try {
					const auto &sql_type = col_type_candidates.back();
					// try vector-cast from string to sql_type
					parse_chunk.data[col];
					Vector dummy_result(GetInternalType(sql_type));
					VectorOperations::Cast(parse_chunk.data[col], dummy_result, SQLType::VARCHAR, sql_type,
					                       parse_chunk.size(), true);
					break;
				} catch (const Exception &e) {
					col_type_candidates.pop_back();
				}
			}
		}
	}

	// information for header detection
	bool first_row_consistent = true;
	bool first_row_nulls = true;

	// parse first row again with knowledge from the rest of the file to check
	// whether first row is consistent with the others or not.
	JumpToBeginning();
	ParseCSV(ParserMode::SNIFFING_DATATYPES);
	if (parse_chunk.size() > 0) {
		for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
			auto dummy_val = parse_chunk.GetValue(col, 0);
			// try cast as SQLNULL
			try {
				dummy_val.CastAs(SQLType::VARCHAR, SQLType::SQLNULL, true);
			} catch (const Exception &e) {
				first_row_nulls = false;
			}
			// try cast to sql_type of column
			vector<SQLType> &col_type_candidates = best_sql_types_candidates[col];
			const auto &sql_type = col_type_candidates.back();

			try {
				dummy_val.CastAs(SQLType::VARCHAR, sql_type, true);
			} catch (const Exception &e) {
				first_row_consistent = false;
				break;
			}
		}
	}

	// if all rows are of type string, we will currently make the assumption there is no header.
	// TODO: Do some kind of string-distance based constistency metic between first row and others
	/*bool all_types_string = true;
	for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
	    const auto &col_type = best_sql_types_candidates[col].back();
	    all_types_string &= (col_type == SQLType::VARCHAR);
	}*/

	// update parser info, and read, generate & set col_names based on previous findings
	if (!first_row_consistent || first_row_nulls) {
		info.header = true;
		vector<string> t_col_names;
		for (idx_t col = 0; col < parse_chunk.column_count(); col++) {
			const auto &val = parse_chunk.GetValue(col, 0);
			string col_name = val.ToString();
			if (col_name.empty() || val.is_null) {
				col_name = GenerateColumnName(parse_chunk.column_count(), col);
			}
			// We'll keep column names as they appear in the file, no canonicalization
			// col_name = StringUtil::Lower(col_name);
			t_col_names.push_back(col_name);
		}
		for (idx_t col = 0; col < t_col_names.size(); col++) {
			string col_name = t_col_names[col];
			idx_t exists_n_times = std::count(t_col_names.begin(), t_col_names.end(), col_name);
			idx_t exists_n_times_before = std::count(t_col_names.begin(), t_col_names.begin() + col, col_name);
			if (exists_n_times > 1) {
				col_name = GenerateColumnName(exists_n_times, exists_n_times_before, col_name + "_");
			}
			col_names.push_back(col_name);
		}
	} else {
		info.header = false;
		idx_t total_columns = parse_chunk.column_count();
		for (idx_t col = 0; col < total_columns; col++) {
			string column_name = GenerateColumnName(total_columns, col);
			col_names.push_back(column_name);
		}
	}

	// set sql types
	vector<SQLType> detected_types;
	for (idx_t col = 0; col < best_sql_types_candidates.size(); col++) {
		SQLType d_type = best_sql_types_candidates[col].back();

		if (requested_types.size() > 0) {
			SQLType r_type = requested_types[col];

			// check if the detected types are in line with the provided types
			if (r_type != d_type) {
				if (r_type.IsMoreGenericThan(d_type)) {
					d_type = r_type;
				} else {
					throw ParserException(
					    "Error while sniffing data type for column '%s': Requested column type %s, detected type %s",
					    col_names[col].c_str(), SQLTypeToString(r_type).c_str(), SQLTypeToString(d_type).c_str());
				}
			}
		}

		detected_types.push_back(d_type);
	}

	// back to normal
	ResetBuffer();
	ResetStream();
	ResetParseChunk();
	sniffed_column_counts.clear();

	return detected_types;
}

void BufferedCSVReader::ParseComplexCSV(DataChunk &insert_chunk) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	vector<idx_t> escape_positions;
	uint8_t delimiter_pos = 0, escape_pos = 0, quote_pos = 0;
	idx_t offset = 0;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}
	// start parsing the first value
	start = position;
	goto value_start;
value_start:
	/* state: value_start */
	// this state parses the first characters of a value
	offset = 0;
	delimiter_pos = 0;
	quote_pos = 0;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (delimiter_pos == info.delimiter.size()) {
				// found a delimiter, add the value
				offset = info.delimiter.size() - 1;
				goto add_value;
			} else if (is_newline(buffer[position])) {
				// found a newline, add the row
				goto add_row;
			}
			if (count > quote_pos) {
				// did not find a quote directly at the start of the value, stop looking for the quote now
				goto normal;
			}
			if (quote_pos == info.quote.size()) {
				// found a quote, go to quoted loop and skip the initial quote
				start += info.quote.size();
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	// file ends while scanning for quote/delimiter, go to final state
	goto final_state;
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	position++;
	do {
		for (; position < buffer_size; position++) {
			delimiter_search.Match(delimiter_pos, buffer[position]);
			if (delimiter_pos == info.delimiter.size()) {
				offset = info.delimiter.size() - 1;
				goto add_value;
			} else if (is_newline(buffer[position])) {
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after newline, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	quote_pos = 0;
	escape_pos = 0;
	position++;
	do {
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			if (quote_pos == info.quote.size()) {
				goto unquote;
			} else if (escape_pos == info.escape.size()) {
				escape_positions.push_back(position - start - (info.escape.size() - 1));
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw ParserException("Error on line %s: unterminated quotes", GetLineNumberStr(linenr, linenr_estimated).c_str());
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	delimiter_pos = 0;
	quote_pos = 0;
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = info.quote.size();
		goto final_state;
	}
	if (is_newline(buffer[position])) {
		// quote followed by newline, add row
		offset = info.quote.size();
		goto add_row;
	}
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (count > delimiter_pos && count > quote_pos) {
				throw ParserException(
				    "Error on line %s: quote should be followed by end of value, end of row or another quote",
				    GetLineNumberStr(linenr, linenr_estimated).c_str());
			}
			if (delimiter_pos == info.delimiter.size()) {
				// quote followed by delimiter, add value
				offset = info.quote.size() + info.delimiter.size() - 1;
				goto add_value;
			} else if (quote_pos == info.quote.size() && (info.escape.size() == 0 || info.escape == info.quote)) {
				// quote followed by quote, go back to quoted state and add to escape
				escape_positions.push_back(position - start - (info.quote.size() - 1));
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	throw ParserException("Error on line %s: quote should be followed by end of value, end of row or another quote",
	                      GetLineNumberStr(linenr, linenr_estimated).c_str());
handle_escape:
	escape_pos = 0;
	quote_pos = 0;
	position++;
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			escape_search.Match(escape_pos, buffer[position]);
			count++;
			if (count > escape_pos && count > quote_pos) {
				throw ParserException("Error on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE",
				                      GetLineNumberStr(linenr, linenr_estimated).c_str());
			}
			if (quote_pos == info.quote.size() || escape_pos == info.escape.size()) {
				// found quote or escape: move back to quoted state
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	throw ParserException("Error on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE",
	                      GetLineNumberStr(linenr, linenr_estimated).c_str());
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after newline, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return;
	}
	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}
	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}
}

void BufferedCSVReader::ParseSimpleCSV(DataChunk &insert_chunk) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	vector<idx_t> escape_positions;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return;
		}
	}
	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == info.quote[0]) {
		// quote: actual value starts in the next position
		// move to in_quotes state
		start = position + 1;
		goto in_quotes;
	} else {
		// no quote, move to normal parsing state
		start = position;
		goto normal;
	}
normal:
	/* state: normal parsing state */
	// this state parses the remainder of a non-quoted value until we reach a delimiter or newline
	do {
		for (; position < buffer_size; position++) {
			if (buffer[position] == info.delimiter[0]) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (is_newline(buffer[position])) {
				// newline: add row
				goto add_row;
			}
		}
	} while (ReadBuffer(start));
	// file ends during normal scan: go to end state
	goto final_state;
add_value:
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	goto value_start;
add_row : {
	// check type of newline (\r or \n)
	bool carriage_return = buffer[position] == '\r';
	AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
	finished_chunk = AddRow(insert_chunk, column);
	// increase position by 1 and move start to the new position
	offset = 0;
	start = ++position;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after delimiter, go to final state
		goto final_state;
	}
	if (carriage_return) {
		// \r newline, go to special state that parses an optional \n afterwards
		goto carriage_return;
	} else {
		// \n newline, move to value start
		if (finished_chunk) {
			return;
		}
		goto value_start;
	}
}
in_quotes:
	/* state: in_quotes */
	// this state parses the remainder of a quoted value
	position++;
	do {
		for (; position < buffer_size; position++) {
			if (buffer[position] == info.quote[0]) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == info.escape[0]) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw ParserException("Error on line %s: unterminated quotes", GetLineNumberStr(linenr, linenr_estimated).c_str());
unquote:
	/* state: unquote */
	// this state handles the state directly after we unquote
	// in this state we expect either another quote (entering the quoted state again, and escaping the quote)
	// or a delimiter/newline, ending the current value and moving on to the next value
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		// file ends right after unquote, go to final state
		offset = 1;
		goto final_state;
	}
	if (buffer[position] == info.quote[0] && (info.escape.size() == 0 || info.escape[0] == info.quote[0])) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == info.delimiter[0]) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (is_newline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		throw ParserException("Error on line %s: quote should be followed by end of value, end of row or another quote",
		                      GetLineNumberStr(linenr, linenr_estimated).c_str());
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		throw ParserException("Error on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE",
		                      GetLineNumberStr(linenr, linenr_estimated).c_str());
	}
	if (buffer[position] != info.quote[0] && buffer[position] != info.escape[0]) {
		throw ParserException("Error on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE",
		                      GetLineNumberStr(linenr, linenr_estimated).c_str());
	}
	// escape was followed by quote or escape, go back to quoted state
	goto in_quotes;
carriage_return:
	/* state: carriage_return */
	// this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
	if (buffer[position] == '\n') {
		// newline after carriage return: skip
		// increase position by 1 and move start to the new position
		start = ++position;
		if (position >= buffer_size && !ReadBuffer(start)) {
			// file ends right after delimiter, go to final state
			goto final_state;
		}
	}
	if (finished_chunk) {
		return;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return;
	}

	if (column > 0 || position > start) {
		// remaining values to be added to the chunk
		AddValue(buffer.get() + start, position - start - offset, column, escape_positions);
		finished_chunk = AddRow(insert_chunk, column);
	}

	// final stage, only reached after parsing the file is finished
	// flush the parsed chunk and finalize parsing
	if (mode == ParserMode::PARSING) {
		Flush(insert_chunk);
	}
}

bool BufferedCSVReader::ReadBuffer(idx_t &start) {
	auto old_buffer = move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;
	idx_t buffer_read_size = INITIAL_BUFFER_SIZE;
	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}
	if (remaining + buffer_read_size > MAXIMUM_CSV_LINE_SIZE) {
		throw ParserException("Maximum line size of %llu bytes exceeded!", MAXIMUM_CSV_LINE_SIZE);
	}
	buffer = unique_ptr<char[]>(new char[buffer_read_size + remaining + 1]);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	source->read(buffer.get() + remaining, buffer_read_size);

	idx_t read_count = source->eof() ? source->gcount() : buffer_read_size;
	bytes_in_chunk += read_count;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(move(old_buffer));
	}
	start = 0;
	position = remaining;

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	cached_buffers.clear();

	ParseCSV(ParserMode::PARSING, insert_chunk);
}

void BufferedCSVReader::ParseCSV(ParserMode parser_mode, DataChunk &insert_chunk) {
	mode = parser_mode;

	if (info.quote.size() <= 1 && info.escape.size() <= 1 && info.delimiter.size() == 1) {
		ParseSimpleCSV(insert_chunk);
	} else {
		ParseComplexCSV(insert_chunk);
	}
}

void BufferedCSVReader::AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions) {
	if (sql_types.size() > 0 && column == sql_types.size() && length == 0) {
		// skip a single trailing delimiter in last column
		return;
	}
	if (mode == ParserMode::SNIFFING_DIALECT) {
		column++;
		return;
	}
	if (column >= sql_types.size()) {
		throw ParserException("Error on line %s: expected %lld values but got %d",
		                      GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(), column + 1);
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	str_val[length] = '\0';
	/*if (info.force_not_null.size() == 0) {
	    info.force_not_null.resize(sql_types.size(), false);
	}*/
	// test against null string
	if (!info.force_not_null[column] && strcmp(info.null_str.c_str(), str_val) == 0) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (escape_positions.size() > 0) {
			// remove escape characters (if any)
			string old_val = str_val;
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);

				if (info.escape.size() == 0 || info.escape == info.quote) {
					prev_pos = next_pos + info.quote.size();
				} else {
					prev_pos = next_pos + info.escape.size();
				}
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddString(v, new_val.c_str(), new_val.size());
		} else {
			parse_data[row_entry] = string_t(str_val, length);
		}
	}

	// move to the next column
	column++;
}

bool BufferedCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column) {
	if (column < sql_types.size() && mode != ParserMode::SNIFFING_DIALECT) {
		throw ParserException("Error on line %s: expected %lld values but got %d",
		                      GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(), column);
	}

	if (mode == ParserMode::SNIFFING_DIALECT) {
		sniffed_column_counts.push_back(column);

		if (sniffed_column_counts.size() == SAMPLE_CHUNK_SIZE) {
			return true;
		}
	} else {
		parse_chunk.SetCardinality(parse_chunk.size() + 1);
	}

	if (mode == ParserMode::SNIFFING_DATATYPES && parse_chunk.size() == SAMPLE_CHUNK_SIZE) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
		return true;
	}

	column = 0;
	linenr++;
	return false;
}

void BufferedCSVReader::Flush(DataChunk &insert_chunk) {
	if (parse_chunk.size() == 0) {
		return;
	}
	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	for (idx_t col_idx = 0; col_idx < sql_types.size(); col_idx++) {
		if (sql_types[col_idx].id == SQLTypeId::VARCHAR) {

			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			auto parse_data = FlatVector::GetData<string_t>(parse_chunk.data[col_idx]);
			for (idx_t i = 0; i < parse_chunk.size(); i++) {
				if (!FlatVector::IsNull(parse_chunk.data[col_idx], i)) {
					auto s = parse_data[i];
					auto utf_type = Utf8Proc::Analyze(s.GetData(), s.GetSize());
					switch (utf_type) {
					case UnicodeType::INVALID:
						throw ParserException("Error on line %s: file is not valid UTF8",
						                      GetLineNumberStr(linenr, linenr_estimated).c_str());
					case UnicodeType::ASCII:
						break;
					case UnicodeType::UNICODE: {
						auto normie = Utf8Proc::Normalize(s.GetData());
						parse_data[i] = StringVector::AddString(parse_chunk.data[col_idx], normie);
						free(normie);
						break;
					}
					}
				}
			}

			insert_chunk.data[col_idx].Reference(parse_chunk.data[col_idx]);
		} else {
			// target type is not varchar: perform a cast
			VectorOperations::Cast(parse_chunk.data[col_idx], insert_chunk.data[col_idx], SQLType::VARCHAR,
			                       sql_types[col_idx], parse_chunk.size());
		}
	}
	parse_chunk.Reset();
}
