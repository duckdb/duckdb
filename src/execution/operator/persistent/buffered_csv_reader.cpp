#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/strftime.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>

namespace duckdb {

static bool ParseBoolean(const Value &value, const string &loption);

static bool ParseBoolean(const vector<Value> &set, const string &loption) {
	if (set.empty()) {
		// no option specified: default to true
		return true;
	}
	if (set.size() > 1) {
		throw BinderException("\"%s\" expects a single argument as a boolean value (e.g. TRUE or 1)", loption);
	}
	return ParseBoolean(set[0], loption);
}

static bool ParseBoolean(const Value &value, const string &loption) {

	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		return ParseBoolean(children, loption);
	}
	if (value.type() == LogicalType::FLOAT || value.type() == LogicalType::DOUBLE ||
	    value.type().id() == LogicalTypeId::DECIMAL) {
		throw BinderException("\"%s\" expects a boolean value (e.g. TRUE or 1)", loption);
	}
	return BooleanValue::Get(value.CastAs(LogicalType::BOOLEAN));
}

static string ParseString(const Value &value, const string &loption) {
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			throw BinderException("\"%s\" expects a single argument as a string value", loption);
		}
		return ParseString(children[0], loption);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("\"%s\" expects a string argument!", loption);
	}
	return value.GetValue<string>();
}

static int64_t ParseInteger(const Value &value, const string &loption) {
	if (value.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(value);
		if (children.size() != 1) {
			// no option specified or multiple options specified
			throw BinderException("\"%s\" expects a single argument as an integer value", loption);
		}
		return ParseInteger(children[0], loption);
	}
	return value.GetValue<int64_t>();
}

static vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &loption) {
	vector<bool> result;

	if (set.empty()) {
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	// list of options: parse the list
	unordered_map<string, bool> option_map;
	for (idx_t i = 0; i < set.size(); i++) {
		option_map[set[i].ToString()] = false;
	}
	result.resize(names.size(), false);
	for (idx_t i = 0; i < names.size(); i++) {
		auto entry = option_map.find(names[i]);
		if (entry != option_map.end()) {
			result[i] = true;
			entry->second = true;
		}
	}
	for (auto &entry : option_map) {
		if (!entry.second) {
			throw BinderException("\"%s\" expected to find %s, but it was not found in the table", loption,
			                      entry.first.c_str());
		}
	}
	return result;
}

static vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &loption) {
	vector<bool> result;

	// Only accept a list of arguments
	if (value.type().id() != LogicalTypeId::LIST) {
		// Support a single argument if it's '*'
		if (value.type().id() == LogicalTypeId::VARCHAR && value.GetValue<string>() == "*") {
			result.resize(names.size(), true);
			return result;
		}
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	auto &children = ListValue::GetChildren(value);
	// accept '*' as single argument
	if (children.size() == 1 && children[0].type().id() == LogicalTypeId::VARCHAR &&
	    children[0].GetValue<string>() == "*") {
		result.resize(names.size(), true);
		return result;
	}
	return ParseColumnList(children, names, loption);
}

struct CSVFileHandle {
public:
	explicit CSVFileHandle(unique_ptr<FileHandle> file_handle_p) : file_handle(move(file_handle_p)) {
		can_seek = file_handle->CanSeek();
		plain_file_source = file_handle->OnDiskFile() && can_seek;
		file_size = file_handle->GetFileSize();
	}

	bool CanSeek() {
		return can_seek;
	}
	void Seek(idx_t position) {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		file_handle->Seek(position);
	}
	idx_t SeekPosition() {
		if (!can_seek) {
			throw InternalException("Cannot seek in this file");
		}
		return file_handle->SeekPosition();
	}
	void Reset() {
		if (plain_file_source) {
			file_handle->Reset();
		} else {
			if (!reset_enabled) {
				throw InternalException("Reset called but reset is not enabled for this CSV Handle");
			}
			read_position = 0;
		}
	}
	bool PlainFileSource() {
		return plain_file_source;
	}

	bool OnDiskFile() {
		return file_handle->OnDiskFile();
	}

	idx_t FileSize() {
		return file_size;
	}

	idx_t Read(void *buffer, idx_t nr_bytes) {
		if (!plain_file_source) {
			// not a plain file source: we need to do some bookkeeping around the reset functionality
			idx_t result_offset = 0;
			if (read_position < buffer_size) {
				// we need to read from our cached buffer
				auto buffer_read_count = MinValue<idx_t>(nr_bytes, buffer_size - read_position);
				memcpy(buffer, cached_buffer.get() + read_position, buffer_read_count);
				result_offset += buffer_read_count;
				read_position += buffer_read_count;
				if (result_offset == nr_bytes) {
					return nr_bytes;
				}
			} else if (!reset_enabled && cached_buffer) {
				// reset is disabled but we still have cached data
				// we can remove any cached data
				cached_buffer.reset();
				buffer_size = 0;
				buffer_capacity = 0;
				read_position = 0;
			}
			// we have data left to read from the file
			// read directly into the buffer
			auto bytes_read = file_handle->Read((char *)buffer + result_offset, nr_bytes - result_offset);
			read_position += bytes_read;
			if (reset_enabled) {
				// if reset caching is enabled, we need to cache the bytes that we have read
				if (buffer_size + bytes_read >= buffer_capacity) {
					// no space; first enlarge the buffer
					buffer_capacity = MaxValue<idx_t>(NextPowerOfTwo(buffer_size + bytes_read), buffer_capacity * 2);

					auto new_buffer = unique_ptr<data_t[]>(new data_t[buffer_capacity]);
					if (buffer_size > 0) {
						memcpy(new_buffer.get(), cached_buffer.get(), buffer_size);
					}
					cached_buffer = move(new_buffer);
				}
				memcpy(cached_buffer.get() + buffer_size, (char *)buffer + result_offset, bytes_read);
				buffer_size += bytes_read;
			}

			return result_offset + bytes_read;
		} else {
			return file_handle->Read(buffer, nr_bytes);
		}
	}

	string ReadLine() {
		string result;
		char buffer[1];
		while (true) {
			idx_t tuples_read = Read(buffer, 1);
			if (tuples_read == 0 || buffer[0] == '\n') {
				return result;
			}
			if (buffer[0] != '\r') {
				result += buffer[0];
			}
		}
	}

	void DisableReset() {
		this->reset_enabled = false;
	}

private:
	unique_ptr<FileHandle> file_handle;
	bool reset_enabled = true;
	bool can_seek = false;
	bool plain_file_source = false;
	idx_t file_size = 0;
	// reset support
	unique_ptr<data_t[]> cached_buffer;
	idx_t read_position = 0;
	idx_t buffer_size = 0;
	idx_t buffer_capacity = 0;
};

void BufferedCSVReaderOptions::SetDelimiter(const string &input) {
	this->delimiter = StringUtil::Replace(input, "\\t", "\t");
	this->has_delimiter = true;
	if (input.empty()) {
		this->delimiter = string("\0", 1);
	}
}

void BufferedCSVReaderOptions::SetDateFormat(LogicalTypeId type, const string &format, bool read_format) {
	string error;
	if (read_format) {
		auto &date_format = this->date_format[type];
		error = StrTimeFormat::ParseFormatSpecifier(format, date_format);
		date_format.format_specifier = format;
	} else {
		auto &date_format = this->write_date_format[type];
		error = StrTimeFormat::ParseFormatSpecifier(format, date_format);
	}
	if (!error.empty()) {
		throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
	}
	has_format[type] = true;
}

void BufferedCSVReaderOptions::SetReadOption(const string &loption, const Value &value,
                                             vector<string> &expected_names) {
	if (SetBaseOption(loption, value)) {
		return;
	}
	if (loption == "auto_detect") {
		auto_detect = ParseBoolean(value, loption);
	} else if (loption == "sample_size") {
		int64_t sample_size = ParseInteger(value, loption);
		if (sample_size < 1 && sample_size != -1) {
			throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
		}
		if (sample_size == -1) {
			sample_chunks = std::numeric_limits<uint64_t>::max();
			sample_chunk_size = STANDARD_VECTOR_SIZE;
		} else if (sample_size <= STANDARD_VECTOR_SIZE) {
			sample_chunk_size = sample_size;
			sample_chunks = 1;
		} else {
			sample_chunk_size = STANDARD_VECTOR_SIZE;
			sample_chunks = sample_size / STANDARD_VECTOR_SIZE;
		}
	} else if (loption == "skip") {
		skip_rows = ParseInteger(value, loption);
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		maximum_line_size = ParseInteger(value, loption);
	} else if (loption == "sample_chunk_size") {
		sample_chunk_size = ParseInteger(value, loption);
		if (sample_chunk_size > STANDARD_VECTOR_SIZE) {
			throw BinderException(
			    "Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be bigger than STANDARD_VECTOR_SIZE %d",
			    STANDARD_VECTOR_SIZE);
		} else if (sample_chunk_size < 1) {
			throw BinderException("Unsupported parameter for SAMPLE_CHUNK_SIZE: cannot be smaller than 1");
		}
	} else if (loption == "sample_chunks") {
		sample_chunks = ParseInteger(value, loption);
		if (sample_chunks < 1) {
			throw BinderException("Unsupported parameter for SAMPLE_CHUNKS: cannot be smaller than 1");
		}
	} else if (loption == "force_not_null") {
		force_not_null = ParseColumnList(value, expected_names, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, true);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, true);
	} else if (loption == "escape") {
		escape = ParseString(value, loption);
		has_escape = true;
	} else if (loption == "ignore_errors") {
		ignore_errors = ParseBoolean(value, loption);
	} else {
		throw BinderException("Unrecognized option for CSV reader \"%s\"", loption);
	}
}

void BufferedCSVReaderOptions::SetWriteOption(const string &loption, const Value &value) {
	if (SetBaseOption(loption, value)) {
		return;
	}

	if (loption == "force_quote") {
		force_quote = ParseColumnList(value, names, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, false);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		if (StringUtil::Lower(format) == "iso") {
			format = "%Y-%m-%dT%H:%M:%S.%fZ";
		}
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, false);
	} else {
		throw BinderException("Unrecognized option CSV writer \"%s\"", loption);
	}
}

bool BufferedCSVReaderOptions::SetBaseOption(const string &loption, const Value &value) {
	// Make sure this function was only called after the option was turned into lowercase
	D_ASSERT(!std::any_of(loption.begin(), loption.end(), ::isupper));

	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		SetDelimiter(ParseString(value, loption));
	} else if (loption == "quote") {
		quote = ParseString(value, loption);
		has_quote = true;
	} else if (loption == "escape") {
		escape = ParseString(value, loption);
		has_escape = true;
	} else if (loption == "header") {
		header = ParseBoolean(value, loption);
		has_header = true;
	} else if (loption == "null" || loption == "nullstr") {
		null_str = ParseString(value, loption);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(value, loption));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else if (loption == "compression") {
		compression = FileCompressionTypeFromString(ParseString(value, loption));
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

std::string BufferedCSVReaderOptions::ToString() const {
	return "DELIMITER='" + delimiter + (has_delimiter ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", QUOTE='" + quote + (has_quote ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", ESCAPE='" + escape + (has_escape ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       ", HEADER=" + std::to_string(header) +
	       (has_header ? "" : (auto_detect ? " (auto detected)" : "' (default)")) +
	       ", SAMPLE_SIZE=" + std::to_string(sample_chunk_size * sample_chunks) +
	       ", IGNORE_ERRORS=" + std::to_string(ignore_errors) + ", ALL_VARCHAR=" + std::to_string(all_varchar);
}

static string GetLineNumberStr(idx_t linenr, bool linenr_estimated) {
	string estimated = (linenr_estimated ? string(" (estimated)") : string(""));
	return to_string(linenr + 1) + estimated;
}

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

	//	replace all dashes with the separator
	for (auto pos = std::find(format_specifier.begin(), format_specifier.end(), '-'); pos != format_specifier.end();
	     pos = std::find(pos + separator.size(), format_specifier.end(), '-')) {
		format_specifier.replace(pos, pos + 1, separator);
	}

	return format_specifier;
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

BufferedCSVReader::BufferedCSVReader(FileSystem &fs_p, Allocator &allocator, FileOpener *opener_p,
                                     BufferedCSVReaderOptions options_p, const vector<LogicalType> &requested_types)
    : fs(fs_p), allocator(allocator), opener(opener_p), options(move(options_p)), buffer_size(0), position(0),
      start(0) {
	file_handle = OpenCSV(options);
	Initialize(requested_types);
}

BufferedCSVReader::BufferedCSVReader(ClientContext &context, BufferedCSVReaderOptions options_p,
                                     const vector<LogicalType> &requested_types)
    : BufferedCSVReader(FileSystem::GetFileSystem(context), Allocator::Get(context), FileSystem::GetFileOpener(context),
                        move(options_p), requested_types) {
}

BufferedCSVReader::~BufferedCSVReader() {
}

idx_t BufferedCSVReader::GetFileSize() {
	return file_handle ? file_handle->FileSize() : 0;
}

void BufferedCSVReader::Initialize(const vector<LogicalType> &requested_types) {
	PrepareComplexParser();
	if (options.auto_detect) {
		sql_types = SniffCSV(requested_types);
		if (sql_types.empty()) {
			throw Exception("Failed to detect column types from CSV: is the file a valid CSV file?");
		}
		if (cached_chunks.empty()) {
			JumpToBeginning(options.skip_rows, options.header);
		}
	} else {
		sql_types = requested_types;
		ResetBuffer();
		SkipRowsAndReadHeader(options.skip_rows, options.header);
	}
	InitParseChunk(sql_types.size());
	// we only need reset support during the automatic CSV type detection
	// since reset support might require caching (in the case of streams), we disable it for the remainder
	file_handle->DisableReset();
}

void BufferedCSVReader::PrepareComplexParser() {
	delimiter_search = TextSearchShiftArray(options.delimiter);
	escape_search = TextSearchShiftArray(options.escape);
	quote_search = TextSearchShiftArray(options.quote);
}

unique_ptr<CSVFileHandle> BufferedCSVReader::OpenCSV(const BufferedCSVReaderOptions &options) {
	auto file_handle = fs.OpenFile(options.file_path.c_str(), FileFlags::FILE_FLAGS_READ, FileLockType::NO_LOCK,
	                               options.compression, this->opener);
	return make_unique<CSVFileHandle>(move(file_handle));
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
	auto nfkd = utf8proc_NFKD((const utf8proc_uint8_t *)col_name.c_str(), col_name.size());
	const string col_name_nfkd = string((const char *)nfkd, strlen((const char *)nfkd));
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

void BufferedCSVReader::ResetBuffer() {
	buffer.reset();
	buffer_size = 0;
	position = 0;
	start = 0;
	cached_buffers.clear();
}

void BufferedCSVReader::ResetStream() {
	if (!file_handle->CanSeek()) {
		// seeking to the beginning appears to not be supported in all compiler/os-scenarios,
		// so we have to create a new stream source here for now
		file_handle->Reset();
	} else {
		file_handle->Seek(0);
	}
	linenr = 0;
	linenr_estimated = false;
	bytes_per_line_avg = 0;
	sample_chunk_idx = 0;
	jumping_samples = false;
}

void BufferedCSVReader::InitParseChunk(idx_t num_cols) {
	// adapt not null info
	if (options.force_not_null.size() != num_cols) {
		options.force_not_null.resize(num_cols, false);
	}
	if (num_cols == parse_chunk.ColumnCount()) {
		parse_chunk.Reset();
	} else {
		parse_chunk.Destroy();

		// initialize the parse_chunk with a set of VARCHAR types
		vector<LogicalType> varchar_types(num_cols, LogicalType::VARCHAR);
		parse_chunk.Initialize(allocator, varchar_types);
	}
}

void BufferedCSVReader::JumpToBeginning(idx_t skip_rows = 0, bool skip_header = false) {
	ResetBuffer();
	ResetStream();
	sample_chunk_idx = 0;
	bytes_in_chunk = 0;
	end_of_file_reached = false;
	bom_checked = false;
	SkipRowsAndReadHeader(skip_rows, skip_header);
}

void BufferedCSVReader::SkipRowsAndReadHeader(idx_t skip_rows, bool skip_header) {
	for (idx_t i = 0; i < skip_rows; i++) {
		// ignore skip rows
		string read_line = file_handle->ReadLine();
		linenr++;
	}

	if (skip_header) {
		// ignore the first line as a header line
		InitParseChunk(sql_types.size());
		ParseCSV(ParserMode::PARSING_HEADER);
	}
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
	if (!file_handle->PlainFileSource() || !jumping_samples) {
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

void BufferedCSVReader::SetDateFormat(const string &format_specifier, const LogicalTypeId &sql_type) {
	options.has_format[sql_type] = true;
	auto &date_format = options.date_format[sql_type];
	date_format.format_specifier = format_specifier;
	StrTimeFormat::ParseFormatSpecifier(date_format.format_specifier, date_format);
}

bool BufferedCSVReader::TryCastValue(const Value &value, const LogicalType &sql_type) {
	if (options.has_format[LogicalTypeId::DATE] && sql_type.id() == LogicalTypeId::DATE) {
		date_t result;
		string error_message;
		return options.date_format[LogicalTypeId::DATE].TryParseDate(string_t(StringValue::Get(value)), result,
		                                                             error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type.id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t result;
		string error_message;
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(string_t(StringValue::Get(value)),
		                                                                       result, error_message);
	} else {
		Value new_value;
		string error_message;
		return value.TryCastAs(sql_type, new_value, &error_message, true);
	}
}

struct TryCastDateOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, date_t &result, string &error_message) {
		return options.date_format[LogicalTypeId::DATE].TryParseDate(input, result, error_message);
	}
};

struct TryCastTimestampOperator {
	static bool Operation(BufferedCSVReaderOptions &options, string_t input, timestamp_t &result,
	                      string &error_message) {
		return options.date_format[LogicalTypeId::TIMESTAMP].TryParseTimestamp(input, result, error_message);
	}
};

template <class OP, class T>
static bool TemplatedTryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
                                       idx_t count, string &error_message) {
	D_ASSERT(input_vector.GetType().id() == LogicalTypeId::VARCHAR);
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, T>(input_vector, result_vector, count, [&](string_t input) {
		T result;
		if (!OP::Operation(options, input, result, error_message)) {
			all_converted = false;
		}
		return result;
	});
	return all_converted;
}

bool TryCastDateVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                       string &error_message) {
	return TemplatedTryCastDateVector<TryCastDateOperator, date_t>(options, input_vector, result_vector, count,
	                                                               error_message);
}

bool TryCastTimestampVector(BufferedCSVReaderOptions &options, Vector &input_vector, Vector &result_vector, idx_t count,
                            string &error_message) {
	return TemplatedTryCastDateVector<TryCastTimestampOperator, timestamp_t>(options, input_vector, result_vector,
	                                                                         count, error_message);
}

bool BufferedCSVReader::TryCastVector(Vector &parse_chunk_col, idx_t size, const LogicalType &sql_type) {
	// try vector-cast from string to sql_type
	Vector dummy_result(sql_type);
	if (options.has_format[LogicalTypeId::DATE] && sql_type == LogicalTypeId::DATE) {
		// use the date format to cast the chunk
		string error_message;
		return TryCastDateVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else if (options.has_format[LogicalTypeId::TIMESTAMP] && sql_type == LogicalTypeId::TIMESTAMP) {
		// use the timestamp format to cast the chunk
		string error_message;
		return TryCastTimestampVector(options, parse_chunk_col, dummy_result, size, error_message);
	} else {
		// target type is not varchar: perform a cast
		string error_message;
		return VectorOperations::TryCast(parse_chunk_col, dummy_result, size, &error_message, true);
	}
}

enum class QuoteRule : uint8_t { QUOTES_RFC = 0, QUOTES_OTHER = 1, NO_QUOTES = 2 };

void BufferedCSVReader::DetectDialect(const vector<LogicalType> &requested_types,
                                      BufferedCSVReaderOptions &original_options,
                                      vector<BufferedCSVReaderOptions> &info_candidates, idx_t &best_num_cols) {
	// set up the candidates we consider for delimiter and quote rules based on user input
	vector<string> delim_candidates;
	vector<QuoteRule> quoterule_candidates;
	vector<vector<string>> quote_candidates_map;
	vector<vector<string>> escape_candidates_map = {{""}, {"\\"}, {""}};

	if (options.has_delimiter) {
		// user provided a delimiter: use that delimiter
		delim_candidates = {options.delimiter};
	} else {
		// no delimiter provided: try standard/common delimiters
		delim_candidates = {",", "|", ";", "\t"};
	}
	if (options.has_quote) {
		// user provided quote: use that quote rule
		quote_candidates_map = {{options.quote}, {options.quote}, {options.quote}};
	} else {
		// no quote rule provided: use standard/common quotes
		quote_candidates_map = {{"\""}, {"\"", "'"}, {""}};
	}
	if (options.has_escape) {
		// user provided escape: use that escape rule
		if (options.escape.empty()) {
			quoterule_candidates = {QuoteRule::QUOTES_RFC};
		} else {
			quoterule_candidates = {QuoteRule::QUOTES_OTHER};
		}
		escape_candidates_map[static_cast<uint8_t>(quoterule_candidates[0])] = {options.escape};
	} else {
		// no escape provided: try standard/common escapes
		quoterule_candidates = {QuoteRule::QUOTES_RFC, QuoteRule::QUOTES_OTHER, QuoteRule::NO_QUOTES};
	}

	idx_t best_consistent_rows = 0;
	for (auto quoterule : quoterule_candidates) {
		const auto &quote_candidates = quote_candidates_map[static_cast<uint8_t>(quoterule)];
		for (const auto &quote : quote_candidates) {
			for (const auto &delim : delim_candidates) {
				const auto &escape_candidates = escape_candidates_map[static_cast<uint8_t>(quoterule)];
				for (const auto &escape : escape_candidates) {
					BufferedCSVReaderOptions sniff_info = original_options;
					sniff_info.delimiter = delim;
					sniff_info.quote = quote;
					sniff_info.escape = escape;

					options = sniff_info;
					PrepareComplexParser();

					JumpToBeginning(original_options.skip_rows);
					sniffed_column_counts.clear();

					if (!TryParseCSV(ParserMode::SNIFFING_DIALECT)) {
						continue;
					}

					idx_t start_row = original_options.skip_rows;
					idx_t consistent_rows = 0;
					idx_t num_cols = 0;

					for (idx_t row = 0; row < sniffed_column_counts.size(); row++) {
						if (sniffed_column_counts[row] == num_cols) {
							consistent_rows++;
						} else {
							num_cols = sniffed_column_counts[row];
							start_row = row + original_options.skip_rows;
							consistent_rows = 1;
						}
					}

					// some logic
					bool more_values = (consistent_rows > best_consistent_rows && num_cols >= best_num_cols);
					bool single_column_before = best_num_cols < 2 && num_cols > best_num_cols;
					bool rows_consistent =
					    start_row + consistent_rows - original_options.skip_rows == sniffed_column_counts.size();
					bool more_than_one_row = (consistent_rows > 1);
					bool more_than_one_column = (num_cols > 1);
					bool start_good = !info_candidates.empty() && (start_row <= info_candidates.front().skip_rows);

					if (!requested_types.empty() && requested_types.size() != num_cols) {
						continue;
					} else if ((more_values || single_column_before) && rows_consistent) {
						sniff_info.skip_rows = start_row;
						sniff_info.num_cols = num_cols;
						best_consistent_rows = consistent_rows;
						best_num_cols = num_cols;

						info_candidates.clear();
						info_candidates.push_back(sniff_info);
					} else if (more_than_one_row && more_than_one_column && start_good && rows_consistent) {
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
}

void BufferedCSVReader::DetectCandidateTypes(const vector<LogicalType> &type_candidates,
                                             const map<LogicalTypeId, vector<const char *>> &format_template_candidates,
                                             const vector<BufferedCSVReaderOptions> &info_candidates,
                                             BufferedCSVReaderOptions &original_options, idx_t best_num_cols,
                                             vector<vector<LogicalType>> &best_sql_types_candidates,
                                             std::map<LogicalTypeId, vector<string>> &best_format_candidates,
                                             DataChunk &best_header_row) {
	BufferedCSVReaderOptions best_options;
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

		// set all sql_types to VARCHAR so we can do datatype detection based on VARCHAR values
		sql_types.clear();
		sql_types.assign(options.num_cols, LogicalType::VARCHAR);

		// jump to beginning and skip potential header
		JumpToBeginning(options.skip_rows, true);
		DataChunk header_row;
		header_row.Initialize(allocator, sql_types);
		parse_chunk.Copy(header_row);

		if (header_row.size() == 0) {
			continue;
		}

		// init parse chunk and read csv with info candidate
		InitParseChunk(sql_types.size());
		ParseCSV(ParserMode::SNIFFING_DATATYPES);
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
						dummy_val = header_row.GetValue(col, 0);
					} else {
						dummy_val = parse_chunk.GetValue(col, row);
					}
					// try formatting for date types if the user did not specify one and it starts with numeric values.
					string separator;
					if (has_format_candidates.count(sql_type.id()) && !original_options.has_format[sql_type.id()] &&
					    StartsWithNumericDate(separator, StringValue::Get(dummy_val))) {
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

			col_names.push_back(col_name);
			name_collision_count[col_name] = 0;
		}

	} else {
		options.header = false;
		for (idx_t col = 0; col < options.num_cols; col++) {
			string column_name = GenerateColumnName(options.num_cols, col);
			col_names.push_back(column_name);
		}
	}
}

vector<LogicalType> BufferedCSVReader::RefineTypeDetection(const vector<LogicalType> &type_candidates,
                                                           const vector<LogicalType> &requested_types,
                                                           vector<vector<LogicalType>> &best_sql_types_candidates,
                                                           map<LogicalTypeId, vector<string>> &best_format_candidates) {
	// for the type refine we set the SQL types to VARCHAR for all columns
	sql_types.clear();
	sql_types.assign(options.num_cols, LogicalType::VARCHAR);

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
		detected_types = sql_types;
	} else {
		// jump through the rest of the file and continue to refine the sql type guess
		while (JumpToNextSample()) {
			InitParseChunk(sql_types.size());
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

			if (!jumping_samples) {
				if ((sample_chunk_idx)*options.sample_chunk_size <= options.buffer_size) {
					// cache parse chunk
					// create a new chunk and fill it with the remainder
					auto chunk = make_unique<DataChunk>();
					auto parse_chunk_types = parse_chunk.GetTypes();
					chunk->Move(parse_chunk);
					cached_chunks.push(move(chunk));
				} else {
					while (!cached_chunks.empty()) {
						cached_chunks.pop();
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

vector<LogicalType> BufferedCSVReader::SniffCSV(const vector<LogicalType> &requested_types) {
	for (auto &type : requested_types) {
		// auto detect for blobs not supported: there may be invalid UTF-8 in the file
		if (type.id() == LogicalTypeId::BLOB) {
			return requested_types;
		}
	}

	// #######
	// ### dialect detection
	// #######
	BufferedCSVReaderOptions original_options = options;
	vector<BufferedCSVReaderOptions> info_candidates;
	idx_t best_num_cols = 0;

	DetectDialect(requested_types, original_options, info_candidates, best_num_cols);

	// if no dialect candidate was found, then file was most likely empty and we throw an exception
	if (info_candidates.empty()) {
		throw InvalidInputException(
		    "Error in file \"%s\": CSV options could not be auto-detected. Consider setting parser options manually.",
		    options.file_path);
	}

	// #######
	// ### type detection (initial)
	// #######
	// type candidates, ordered by descending specificity (~ from high to low)
	vector<LogicalType> type_candidates = {
	    LogicalType::VARCHAR, LogicalType::TIMESTAMP,
	    LogicalType::DATE,    LogicalType::TIME,
	    LogicalType::DOUBLE,  /* LogicalType::FLOAT,*/ LogicalType::BIGINT,
	    LogicalType::INTEGER, /*LogicalType::SMALLINT, LogicalType::TINYINT,*/ LogicalType::BOOLEAN,
	    LogicalType::SQLNULL};
	// format template candidates, ordered by descending specificity (~ from high to low)
	std::map<LogicalTypeId, vector<const char *>> format_template_candidates = {
	    {LogicalTypeId::DATE, {"%m-%d-%Y", "%m-%d-%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d", "%y-%m-%d"}},
	    {LogicalTypeId::TIMESTAMP,
	     {"%Y-%m-%d %H:%M:%S.%f", "%m-%d-%Y %I:%M:%S %p", "%m-%d-%y %I:%M:%S %p", "%d-%m-%Y %H:%M:%S",
	      "%d-%m-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%y-%m-%d %H:%M:%S"}},
	};
	vector<vector<LogicalType>> best_sql_types_candidates;
	map<LogicalTypeId, vector<string>> best_format_candidates;
	DataChunk best_header_row;
	DetectCandidateTypes(type_candidates, format_template_candidates, info_candidates, original_options, best_num_cols,
	                     best_sql_types_candidates, best_format_candidates, best_header_row);

	// #######
	// ### header detection
	// #######
	options.num_cols = best_num_cols;
	DetectHeader(best_sql_types_candidates, best_header_row);

	// #######
	// ### type detection (refining)
	// #######
	return RefineTypeDetection(type_candidates, requested_types, best_sql_types_candidates, best_format_candidates);
}

bool BufferedCSVReader::TryParseComplexCSV(DataChunk &insert_chunk, string &error_message) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	vector<idx_t> escape_positions;
	uint8_t delimiter_pos = 0, escape_pos = 0, quote_pos = 0;
	idx_t offset = 0;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return true;
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
			if (delimiter_pos == options.delimiter.size()) {
				// found a delimiter, add the value
				offset = options.delimiter.size() - 1;
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
				// found a newline, add the row
				goto add_row;
			}
			if (count > quote_pos) {
				// did not find a quote directly at the start of the value, stop looking for the quote now
				goto normal;
			}
			if (quote_pos == options.quote.size()) {
				// found a quote, go to quoted loop and skip the initial quote
				start += options.quote.size();
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
			if (delimiter_pos == options.delimiter.size()) {
				offset = options.delimiter.size() - 1;
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
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
			return true;
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
			if (quote_pos == options.quote.size()) {
				goto unquote;
			} else if (escape_pos == options.escape.size()) {
				escape_positions.push_back(position - start - (options.escape.size() - 1));
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	error_message = StringUtil::Format("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                                   GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
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
		offset = options.quote.size();
		goto final_state;
	}
	if (StringUtil::CharacterIsNewline(buffer[position])) {
		// quote followed by newline, add row
		offset = options.quote.size();
		goto add_row;
	}
	do {
		idx_t count = 0;
		for (; position < buffer_size; position++) {
			quote_search.Match(quote_pos, buffer[position]);
			delimiter_search.Match(delimiter_pos, buffer[position]);
			count++;
			if (count > delimiter_pos && count > quote_pos) {
				error_message = StringUtil::Format(
				    "Error in file \"%s\" on line %s: quote should be followed by end of value, end "
				    "of row or another quote. (%s)",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
				return false;
			}
			if (delimiter_pos == options.delimiter.size()) {
				// quote followed by delimiter, add value
				offset = options.quote.size() + options.delimiter.size() - 1;
				goto add_value;
			} else if (quote_pos == options.quote.size() &&
			           (options.escape.empty() || options.escape == options.quote)) {
				// quote followed by quote, go back to quoted state and add to escape
				escape_positions.push_back(position - start - (options.quote.size() - 1));
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	error_message = StringUtil::Format(
	    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of row or another quote. (%s)",
	    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
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
				error_message = StringUtil::Format(
				    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)",
				    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
				return false;
			}
			if (quote_pos == options.quote.size() || escape_pos == options.escape.size()) {
				// found quote or escape: move back to quoted state
				goto in_quotes;
			}
		}
	} while (ReadBuffer(start));
	error_message =
	    StringUtil::Format("Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)",
	                       options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
	return false;
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
		return true;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
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

	end_of_file_reached = true;
	return true;
}

bool BufferedCSVReader::TryParseSimpleCSV(DataChunk &insert_chunk, string &error_message) {
	// used for parsing algorithm
	bool finished_chunk = false;
	idx_t column = 0;
	idx_t offset = 0;
	vector<idx_t> escape_positions;

	// read values into the buffer (if any)
	if (position >= buffer_size) {
		if (!ReadBuffer(start)) {
			return true;
		}
	}
	// start parsing the first value
	goto value_start;
value_start:
	offset = 0;
	/* state: value_start */
	// this state parses the first character of a value
	if (buffer[position] == options.quote[0]) {
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
			if (buffer[position] == options.delimiter[0]) {
				// delimiter: end the value and add it to the chunk
				goto add_value;
			} else if (StringUtil::CharacterIsNewline(buffer[position])) {
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
			return true;
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
			if (buffer[position] == options.quote[0]) {
				// quote: move to unquoted state
				goto unquote;
			} else if (buffer[position] == options.escape[0]) {
				// escape: store the escaped position and move to handle_escape state
				escape_positions.push_back(position - start);
				goto handle_escape;
			}
		}
	} while (ReadBuffer(start));
	// still in quoted state at the end of the file, error:
	throw InvalidInputException("Error in file \"%s\" on line %s: unterminated quotes. (%s)", options.file_path,
	                            GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
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
	if (buffer[position] == options.quote[0] && (options.escape.empty() || options.escape[0] == options.quote[0])) {
		// escaped quote, return to quoted state and store escape position
		escape_positions.push_back(position - start);
		goto in_quotes;
	} else if (buffer[position] == options.delimiter[0]) {
		// delimiter, add value
		offset = 1;
		goto add_value;
	} else if (StringUtil::CharacterIsNewline(buffer[position])) {
		offset = 1;
		goto add_row;
	} else {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: quote should be followed by end of value, end of "
		    "row or another quote. (%s)",
		    options.file_path, GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
handle_escape:
	/* state: handle_escape */
	// escape should be followed by a quote or another escape character
	position++;
	if (position >= buffer_size && !ReadBuffer(start)) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
	}
	if (buffer[position] != options.quote[0] && buffer[position] != options.escape[0]) {
		error_message = StringUtil::Format(
		    "Error in file \"%s\" on line %s: neither QUOTE nor ESCAPE is proceeded by ESCAPE. (%s)", options.file_path,
		    GetLineNumberStr(linenr, linenr_estimated).c_str(), options.ToString());
		return false;
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
		return true;
	}
	goto value_start;
final_state:
	if (finished_chunk) {
		return true;
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

	end_of_file_reached = true;
	return true;
}

bool BufferedCSVReader::ReadBuffer(idx_t &start) {
	auto old_buffer = move(buffer);

	// the remaining part of the last buffer
	idx_t remaining = buffer_size - start;

	bool large_buffers = mode == ParserMode::PARSING && !file_handle->OnDiskFile() && file_handle->CanSeek();
	idx_t buffer_read_size = large_buffers ? INITIAL_BUFFER_SIZE_LARGE : INITIAL_BUFFER_SIZE;

	while (remaining > buffer_read_size) {
		buffer_read_size *= 2;
	}

	// Check line length
	if (remaining > options.maximum_line_size) {
		throw InvalidInputException("Maximum line size of %llu bytes exceeded!", options.maximum_line_size);
	}

	buffer = unique_ptr<char[]>(new char[buffer_read_size + remaining + 1]);
	buffer_size = remaining + buffer_read_size;
	if (remaining > 0) {
		// remaining from last buffer: copy it here
		memcpy(buffer.get(), old_buffer.get() + start, remaining);
	}
	idx_t read_count = file_handle->Read(buffer.get() + remaining, buffer_read_size);

	bytes_in_chunk += read_count;
	buffer_size = remaining + read_count;
	buffer[buffer_size] = '\0';
	if (old_buffer) {
		cached_buffers.push_back(move(old_buffer));
	}
	start = 0;
	position = remaining;
	if (!bom_checked) {
		bom_checked = true;
		if (read_count >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
			position += 3;
		}
	}

	return read_count > 0;
}

void BufferedCSVReader::ParseCSV(DataChunk &insert_chunk) {
	// if no auto-detect or auto-detect with jumping samples, we have nothing cached and start from the beginning
	if (cached_chunks.empty()) {
		cached_buffers.clear();
	} else {
		auto &chunk = cached_chunks.front();
		parse_chunk.Move(*chunk);
		cached_chunks.pop();
		Flush(insert_chunk);
		return;
	}

	string error_message;
	if (!TryParseCSV(ParserMode::PARSING, insert_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	return TryParseCSV(mode, dummy_chunk, error_message);
}

void BufferedCSVReader::ParseCSV(ParserMode mode) {
	DataChunk dummy_chunk;
	string error_message;
	if (!TryParseCSV(mode, dummy_chunk, error_message)) {
		throw InvalidInputException(error_message);
	}
}

bool BufferedCSVReader::TryParseCSV(ParserMode parser_mode, DataChunk &insert_chunk, string &error_message) {
	mode = parser_mode;

	if (options.quote.size() <= 1 && options.escape.size() <= 1 && options.delimiter.size() == 1) {
		return TryParseSimpleCSV(insert_chunk, error_message);
	} else {
		return TryParseComplexCSV(insert_chunk, error_message);
	}
}

void BufferedCSVReader::AddValue(char *str_val, idx_t length, idx_t &column, vector<idx_t> &escape_positions) {
	if (length == 0 && column == 0) {
		row_empty = true;
	} else {
		row_empty = false;
	}

	if (!sql_types.empty() && column == sql_types.size() && length == 0) {
		// skip a single trailing delimiter in last column
		return;
	}
	if (mode == ParserMode::SNIFFING_DIALECT) {
		column++;
		return;
	}
	if (column >= sql_types.size()) {
		if (options.ignore_errors) {
			error_column_overflow = true;
			return;
		} else {
			throw InvalidInputException("Error on line %s: expected %lld values per row, but got more. (%s)",
			                            GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(),
			                            options.ToString());
		}
	}

	// insert the line number into the chunk
	idx_t row_entry = parse_chunk.size();

	str_val[length] = '\0';

	// test against null string
	if (!options.force_not_null[column] && strcmp(options.null_str.c_str(), str_val) == 0) {
		FlatVector::SetNull(parse_chunk.data[column], row_entry, true);
	} else {
		auto &v = parse_chunk.data[column];
		auto parse_data = FlatVector::GetData<string_t>(v);
		if (!escape_positions.empty()) {
			// remove escape characters (if any)
			string old_val = str_val;
			string new_val = "";
			idx_t prev_pos = 0;
			for (idx_t i = 0; i < escape_positions.size(); i++) {
				idx_t next_pos = escape_positions[i];
				new_val += old_val.substr(prev_pos, next_pos - prev_pos);

				if (options.escape.empty() || options.escape == options.quote) {
					prev_pos = next_pos + options.quote.size();
				} else {
					prev_pos = next_pos + options.escape.size();
				}
			}
			new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
			escape_positions.clear();
			parse_data[row_entry] = StringVector::AddStringOrBlob(v, string_t(new_val));
		} else {
			parse_data[row_entry] = string_t(str_val, length);
		}
	}

	// move to the next column
	column++;
}

bool BufferedCSVReader::AddRow(DataChunk &insert_chunk, idx_t &column) {
	linenr++;

	if (row_empty) {
		row_empty = false;
		if (sql_types.size() != 1) {
			column = 0;
			return false;
		}
	}

	// Error forwarded by 'ignore_errors' - originally encountered in 'AddValue'
	if (error_column_overflow) {
		D_ASSERT(options.ignore_errors);
		error_column_overflow = false;
		column = 0;
		return false;
	}

	if (column < sql_types.size() && mode != ParserMode::SNIFFING_DIALECT) {
		if (options.ignore_errors) {
			column = 0;
			return false;
		} else {
			throw InvalidInputException("Error on line %s: expected %lld values per row, but got %d. (%s)",
			                            GetLineNumberStr(linenr, linenr_estimated).c_str(), sql_types.size(), column,
			                            options.ToString());
		}
	}

	if (mode == ParserMode::SNIFFING_DIALECT) {
		sniffed_column_counts.push_back(column);

		if (sniffed_column_counts.size() == options.sample_chunk_size) {
			return true;
		}
	} else {
		parse_chunk.SetCardinality(parse_chunk.size() + 1);
	}

	if (mode == ParserMode::PARSING_HEADER) {
		return true;
	}

	if (mode == ParserMode::SNIFFING_DATATYPES && parse_chunk.size() == options.sample_chunk_size) {
		return true;
	}

	if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
		Flush(insert_chunk);
		return true;
	}

	column = 0;
	return false;
}

void BufferedCSVReader::Flush(DataChunk &insert_chunk) {
	if (parse_chunk.size() == 0) {
		return;
	}

	bool conversion_error_ignored = false;

	// convert the columns in the parsed chunk to the types of the table
	insert_chunk.SetCardinality(parse_chunk);
	for (idx_t col_idx = 0; col_idx < sql_types.size(); col_idx++) {
		if (sql_types[col_idx].id() == LogicalTypeId::VARCHAR) {
			// target type is varchar: no need to convert
			// just test that all strings are valid utf-8 strings
			auto parse_data = FlatVector::GetData<string_t>(parse_chunk.data[col_idx]);
			for (idx_t i = 0; i < parse_chunk.size(); i++) {
				if (!FlatVector::IsNull(parse_chunk.data[col_idx], i)) {
					auto s = parse_data[i];
					auto utf_type = Utf8Proc::Analyze(s.GetDataUnsafe(), s.GetSize());
					if (utf_type == UnicodeType::INVALID) {
						string col_name = to_string(col_idx);
						if (col_idx < col_names.size()) {
							col_name = "\"" + col_names[col_idx] + "\"";
						}
						throw InvalidInputException("Error in file \"%s\" between line %llu and %llu in column \"%s\": "
						                            "file is not valid UTF8. Parser options: %s",
						                            options.file_path, linenr - parse_chunk.size(), linenr, col_name,
						                            options.ToString());
					}
				}
			}
			insert_chunk.data[col_idx].Reference(parse_chunk.data[col_idx]);
		} else {
			string error_message;
			bool success;
			if (options.has_format[LogicalTypeId::DATE] && sql_types[col_idx].id() == LogicalTypeId::DATE) {
				// use the date format to cast the chunk
				success = TryCastDateVector(options, parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                            parse_chunk.size(), error_message);
			} else if (options.has_format[LogicalTypeId::TIMESTAMP] &&
			           sql_types[col_idx].id() == LogicalTypeId::TIMESTAMP) {
				// use the date format to cast the chunk
				success = TryCastTimestampVector(options, parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                                 parse_chunk.size(), error_message);
			} else {
				// target type is not varchar: perform a cast
				success = VectorOperations::TryCast(parse_chunk.data[col_idx], insert_chunk.data[col_idx],
				                                    parse_chunk.size(), &error_message);
			}
			if (success) {
				continue;
			}
			if (options.ignore_errors) {
				conversion_error_ignored = true;
				continue;
			}
			string col_name = to_string(col_idx);
			if (col_idx < col_names.size()) {
				col_name = "\"" + col_names[col_idx] + "\"";
			}

			if (options.auto_detect) {
				throw InvalidInputException("%s in column %s, between line %llu and %llu. Parser "
				                            "options: %s. Consider either increasing the sample size "
				                            "(SAMPLE_SIZE=X [X rows] or SAMPLE_SIZE=-1 [all rows]), "
				                            "or skipping column conversion (ALL_VARCHAR=1)",
				                            error_message, col_name, linenr - parse_chunk.size() + 1, linenr,
				                            options.ToString());
			} else {
				throw InvalidInputException("%s between line %llu and %llu in column %s. Parser options: %s ",
				                            error_message, linenr - parse_chunk.size(), linenr, col_name,
				                            options.ToString());
			}
		}
	}
	if (conversion_error_ignored) {
		D_ASSERT(options.ignore_errors);
		SelectionVector succesful_rows;
		succesful_rows.Initialize(parse_chunk.size());
		idx_t sel_size = 0;

		for (idx_t row_idx = 0; row_idx < parse_chunk.size(); row_idx++) {
			bool failed = false;
			for (idx_t column_idx = 0; column_idx < sql_types.size(); column_idx++) {

				auto &inserted_column = insert_chunk.data[column_idx];
				auto &parsed_column = parse_chunk.data[column_idx];

				bool was_already_null = FlatVector::IsNull(parsed_column, row_idx);
				if (!was_already_null && FlatVector::IsNull(inserted_column, row_idx)) {
					failed = true;
					break;
				}
			}
			if (!failed) {
				succesful_rows.set_index(sel_size++, row_idx);
			}
		}
		insert_chunk.Slice(succesful_rows, sel_size);
	}
	parse_chunk.Reset();
}
} // namespace duckdb
