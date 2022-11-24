#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/string_util.hpp"

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
	return BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
}

static string ParseString(const Value &value, const string &loption) {
	if (value.IsNull()) {
		return string();
	}
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

void BufferedCSVReaderOptions::SetDelimiter(const string &input) {
	this->delimiter = StringUtil::Replace(input, "\\t", "\t");
	this->has_delimiter = true;
	if (input.empty()) {
		this->delimiter = string("\0", 1);
	}
}

void BufferedCSVReaderOptions::SetNewline(const string &input) {
	if (input == "\\n") {
		this->new_line = NewLineIdentifier::N;
	} else if (input == "\\r") {
		this->new_line = NewLineIdentifier::R;
	} else if (input == "\\r\\n") {
		this->new_line = NewLineIdentifier::RN;
	} else {
		throw InvalidInputException("This is not accepted as a newline: " + input);
	}
	this->has_newline = true;
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
	} else if (loption == "union_by_name") {
		union_by_name = ParseBoolean(value, loption);
	} else if (loption == "buffer_size") {
		buffer_size = ParseInteger(value, loption);
		if (buffer_size == 0) {
			throw InvalidInputException("Buffer Size option must be higher than 0");
		}
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

} // namespace duckdb
