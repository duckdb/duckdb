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

// CSV Sniffing consists of four steps:
// 1. Dialect Detection: Generate the CSV Options (delimiter, quote, escape, etc.)
// 2. Type Detection: Figures out the types of the columns
// 3. Header Detection: Figures out if  the CSV file has a header and produces the names of the columns
// 4. Type Refinement: Refines the types of the columns
SnifferResult CSVSniffer::SniffCSV() {
	// 1. Dialect Detection
	DetectDialect();
	// 2. Type Detection
	DetectTypes();
	// 3. Header Detection
	// 4. Type Refinement
	// We are done, construct and return the result.
	return SnifferResult();
}

// vector<LogicalType> BufferedCSVReader::SniffCSV(const vector<LogicalType> &requested_types) {

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
