#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/common/bind_helpers.hpp"
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

void CSVReaderOptions::SetHeader(bool input) {
	this->dialect_options.header = input;
	this->has_header = true;
}

void CSVReaderOptions::SetCompression(const string &compression_p) {
	this->compression = FileCompressionTypeFromString(compression_p);
}

void CSVReaderOptions::SetEscape(const string &input) {
	auto escape_str = input;
	if (escape_str.size() > 1) {
		throw InvalidInputException("The escape option cannot exceed a size of 1 byte.");
	}
	if (escape_str.empty()) {
		escape_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.escape = escape_str[0];
	this->has_escape = true;
}

void CSVReaderOptions::SetDelimiter(const string &input) {
	auto delim_str = StringUtil::Replace(input, "\\t", "\t");
	if (delim_str.size() > 1) {
		throw InvalidInputException("The delimiter option cannot exceed a size of 1 byte.");
	}
	this->has_delimiter = true;
	if (input.empty()) {
		delim_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.delimiter = delim_str[0];
}

void CSVReaderOptions::SetQuote(const string &quote_p) {
	auto quote_str = quote_p;
	if (quote_str.size() > 1) {
		throw InvalidInputException("The quote option cannot exceed a size of 1 byte.");
	}
	if (quote_str.empty()) {
		quote_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.quote = quote_str[0];
	this->has_quote = true;
}

void CSVReaderOptions::SetNewline(const string &input) {
	if (input == "\\n" || input == "\\r") {
		dialect_options.new_line = NewLineIdentifier::SINGLE;
	} else if (input == "\\r\\n") {
		dialect_options.new_line = NewLineIdentifier::CARRY_ON;
	} else {
		throw InvalidInputException("This is not accepted as a newline: " + input);
	}
	has_newline = true;
}

void CSVReaderOptions::SetDateFormat(LogicalTypeId type, const string &format, bool read_format) {
	string error;
	if (read_format) {
		error = StrTimeFormat::ParseFormatSpecifier(format, dialect_options.date_format[type]);
		dialect_options.date_format[type].format_specifier = format;
	} else {
		error = StrTimeFormat::ParseFormatSpecifier(format, write_date_format[type]);
	}
	if (!error.empty()) {
		throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
	}
	dialect_options.has_format[type] = true;
}

void CSVReaderOptions::SetReadOption(const string &loption, const Value &value, vector<string> &expected_names) {
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
			sample_chunks = sample_size / STANDARD_VECTOR_SIZE + 1;
		}
	} else if (loption == "skip") {
		dialect_options.skip_rows = ParseInteger(value, loption);
		skip_rows_set = true;
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
	} else if (loption == "ignore_errors") {
		ignore_errors = ParseBoolean(value, loption);
	} else if (loption == "buffer_size") {
		buffer_size = ParseInteger(value, loption);
		if (buffer_size == 0) {
			throw InvalidInputException("Buffer Size option must be higher than 0");
		}
	} else if (loption == "decimal_separator") {
		decimal_separator = ParseString(value, loption);
		if (decimal_separator != "." && decimal_separator != ",") {
			throw BinderException("Unsupported parameter for DECIMAL_SEPARATOR: should be '.' or ','");
		}
	} else if (loption == "null_padding") {
		null_padding = ParseBoolean(value, loption);
	} else if (loption == "allow_quoted_nulls") {
		allow_quoted_nulls = ParseBoolean(value, loption);
	} else if (loption == "parallel") {
		parallel_mode = ParseBoolean(value, loption) ? ParallelMode::PARALLEL : ParallelMode::SINGLE_THREADED;
	} else if (loption == "rejects_table") {
		// skip, handled in SetRejectsOptions
		auto table_name = ParseString(value, loption);
		if (table_name.empty()) {
			throw BinderException("REJECTS_TABLE option cannot be empty");
		}
		rejects_table_name = table_name;
	} else if (loption == "rejects_recovery_columns") {
		// Get the list of columns to use as a recovery key
		auto &children = ListValue::GetChildren(value);
		for (auto &child : children) {
			auto col_name = child.GetValue<string>();
			rejects_recovery_columns.push_back(col_name);
		}
	} else if (loption == "rejects_limit") {
		int64_t limit = ParseInteger(value, loption);
		if (limit < 0) {
			throw BinderException("Unsupported parameter for REJECTS_LIMIT: cannot be negative");
		}
		rejects_limit = limit;
	} else {
		throw BinderException("Unrecognized option for CSV reader \"%s\"", loption);
	}
}

void CSVReaderOptions::SetWriteOption(const string &loption, const Value &value) {
	if (loption == "new_line") {
		// Steal this from SetBaseOption so we can write different newlines (e.g., format JSON ARRAY)
		write_newline = ParseString(value, loption);
		return;
	}

	if (SetBaseOption(loption, value)) {
		return;
	}

	if (loption == "force_quote") {
		force_quote = ParseColumnList(value, name_list, loption);
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, false);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		if (StringUtil::Lower(format) == "iso") {
			format = "%Y-%m-%dT%H:%M:%S.%fZ";
		}
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, false);
		SetDateFormat(LogicalTypeId::TIMESTAMP_TZ, format, false);
	} else if (loption == "prefix") {
		prefix = ParseString(value, loption);
	} else if (loption == "suffix") {
		suffix = ParseString(value, loption);
	} else {
		throw BinderException("Unrecognized option CSV writer \"%s\"", loption);
	}
}

bool CSVReaderOptions::SetBaseOption(const string &loption, const Value &value) {
	// Make sure this function was only called after the option was turned into lowercase
	D_ASSERT(!std::any_of(loption.begin(), loption.end(), ::isupper));

	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		SetDelimiter(ParseString(value, loption));
	} else if (loption == "quote") {
		SetQuote(ParseString(value, loption));
	} else if (loption == "new_line") {
		SetNewline(ParseString(value, loption));
	} else if (loption == "escape") {
		SetEscape(ParseString(value, loption));
	} else if (loption == "header") {
		SetHeader(ParseBoolean(value, loption));
	} else if (loption == "null" || loption == "nullstr") {
		null_str = ParseString(value, loption);
	} else if (loption == "encoding") {
		auto encoding = StringUtil::Lower(ParseString(value, loption));
		if (encoding != "utf8" && encoding != "utf-8") {
			throw BinderException("Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'");
		}
	} else if (loption == "compression") {
		SetCompression(ParseString(value, loption));
	} else {
		// unrecognized option in base CSV
		return false;
	}
	return true;
}

string CSVReaderOptions::ToString() const {
	return "  file=" + file_path + "\n  delimiter='" + dialect_options.state_machine_options.delimiter +
	       (has_delimiter ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) + "\n  quote='" +
	       dialect_options.state_machine_options.quote +
	       (has_quote ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) + "\n  escape='" +
	       dialect_options.state_machine_options.escape +
	       (has_escape ? "'" : (auto_detect ? "' (auto detected)" : "' (default)")) +
	       "\n  header=" + std::to_string(dialect_options.header) +
	       (has_header ? "" : (auto_detect ? " (auto detected)" : "' (default)")) +
	       "\n  sample_size=" + std::to_string(sample_chunk_size * sample_chunks) +
	       "\n  ignore_errors=" + std::to_string(ignore_errors) + "\n  all_varchar=" + std::to_string(all_varchar);
}

} // namespace duckdb
