#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"

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

bool CSVReaderOptions::GetHeader() const {
	return this->dialect_options.header;
}

void CSVReaderOptions::SetHeader(bool input) {
	this->dialect_options.header = input;
	this->has_header = true;
}

void CSVReaderOptions::SetCompression(const string &compression_p) {
	this->compression = FileCompressionTypeFromString(compression_p);
}

string CSVReaderOptions::GetEscape() const {
	return std::string(1, this->dialect_options.state_machine_options.escape);
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

int64_t CSVReaderOptions::GetSkipRows() const {
	return this->dialect_options.skip_rows;
}

void CSVReaderOptions::SetSkipRows(int64_t skip_rows) {
	dialect_options.skip_rows = skip_rows;
	skip_rows_set = true;
}

string CSVReaderOptions::GetDelimiter() const {
	return std::string(1, this->dialect_options.state_machine_options.delimiter);
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

string CSVReaderOptions::GetQuote() const {
	return std::string(1, this->dialect_options.state_machine_options.quote);
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

NewLineIdentifier CSVReaderOptions::GetNewline() const {
	return dialect_options.new_line;
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
		int64_t sample_size_option = ParseInteger(value, loption);
		if (sample_size_option < 1 && sample_size_option != -1) {
			throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
		}
		if (sample_size_option == -1) {
			// If -1, we basically read the whole thing
			sample_size_chunks = NumericLimits<idx_t>().Maximum();
		} else {
			sample_size_chunks = sample_size_option / STANDARD_VECTOR_SIZE;
			if (sample_size_option % STANDARD_VECTOR_SIZE != 0) {
				sample_size_chunks++;
			}
		}

	} else if (loption == "skip") {
		SetSkipRows(ParseInteger(value, loption));
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		maximum_line_size = ParseInteger(value, loption);
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
	       "\n  sample_size=" + std::to_string(sample_size_chunks * STANDARD_VECTOR_SIZE) +
	       "\n  ignore_errors=" + std::to_string(ignore_errors) + "\n  all_varchar=" + std::to_string(all_varchar);
}

static Value StringVectorToValue(const vector<string> &vec) {
	vector<Value> content;
	content.reserve(vec.size());
	for (auto &item : vec) {
		content.push_back(Value(item));
	}
	return Value::LIST(std::move(content));
}

static uint8_t GetCandidateSpecificity(const LogicalType &candidate_type) {
	//! Const ht with accepted auto_types and their weights in specificity
	const duckdb::unordered_map<uint8_t, uint8_t> auto_type_candidates_specificity {
	    {(uint8_t)LogicalTypeId::VARCHAR, 0},  {(uint8_t)LogicalTypeId::TIMESTAMP, 1},
	    {(uint8_t)LogicalTypeId::DATE, 2},     {(uint8_t)LogicalTypeId::TIME, 3},
	    {(uint8_t)LogicalTypeId::DOUBLE, 4},   {(uint8_t)LogicalTypeId::FLOAT, 5},
	    {(uint8_t)LogicalTypeId::BIGINT, 6},   {(uint8_t)LogicalTypeId::INTEGER, 7},
	    {(uint8_t)LogicalTypeId::SMALLINT, 8}, {(uint8_t)LogicalTypeId::TINYINT, 9},
	    {(uint8_t)LogicalTypeId::BOOLEAN, 10}, {(uint8_t)LogicalTypeId::SQLNULL, 11}};

	auto id = (uint8_t)candidate_type.id();
	auto it = auto_type_candidates_specificity.find(id);
	if (it == auto_type_candidates_specificity.end()) {
		throw BinderException("Auto Type Candidate of type %s is not accepted as a valid input",
		                      EnumUtil::ToString(candidate_type.id()));
	}
	return it->second;
}

void CSVReaderOptions::FromNamedParameters(named_parameter_map_t &in, ClientContext &context,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	for (auto &kv : in) {
		if (MultiFileReader::ParseOption(kv.first, kv.second, file_options, context)) {
			continue;
		}
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "columns") {
			explicitly_set_columns = true;
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_csv columns requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				names.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				return_types.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			if (names.empty()) {
				throw BinderException("read_csv requires at least a single column as input!");
			}
		} else if (loption == "auto_type_candidates") {
			auto_type_candidates.clear();
			map<uint8_t, LogicalType> candidate_types;
			// We always have the extremes of Null and Varchar, so we can default to varchar if the
			// sniffer is not able to confidently detect that column type
			candidate_types[GetCandidateSpecificity(LogicalType::VARCHAR)] = LogicalType::VARCHAR;
			candidate_types[GetCandidateSpecificity(LogicalType::SQLNULL)] = LogicalType::SQLNULL;

			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv auto_types requires a list as input");
			}
			auto &list_children = ListValue::GetChildren(kv.second);
			if (list_children.empty()) {
				throw BinderException("auto_type_candidates requires at least one type");
			}
			for (auto &child : list_children) {
				if (child.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("auto_type_candidates requires a type specification as string");
				}
				auto candidate_type = TransformStringToLogicalType(StringValue::Get(child), context);
				candidate_types[GetCandidateSpecificity(candidate_type)] = candidate_type;
			}
			for (auto &candidate_type : candidate_types) {
				auto_type_candidates.emplace_back(candidate_type.second);
			}
		} else if (loption == "column_names" || loption == "names") {
			if (!name_list.empty()) {
				throw BinderException("read_csv_auto column_names/names can only be supplied once");
			}
			if (kv.second.IsNull()) {
				throw BinderException("read_csv_auto %s cannot be NULL", kv.first);
			}
			auto &children = ListValue::GetChildren(kv.second);
			for (auto &child : children) {
				name_list.push_back(StringValue::Get(child));
			}
		} else if (loption == "column_types" || loption == "types" || loption == "dtypes") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT && child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv_auto %s requires a struct or list as input", kv.first);
			}
			if (!sql_type_list.empty()) {
				throw BinderException("read_csv_auto column_types/types/dtypes can only be supplied once");
			}
			vector<string> sql_type_names;
			if (child_type.id() == LogicalTypeId::STRUCT) {
				auto &struct_children = StructValue::GetChildren(kv.second);
				D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
				for (idx_t i = 0; i < struct_children.size(); i++) {
					auto &name = StructType::GetChildName(child_type, i);
					auto &val = struct_children[i];
					if (val.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("read_csv_auto %s requires a type specification as string", kv.first);
					}
					sql_type_names.push_back(StringValue::Get(val));
					sql_types_per_column[name] = i;
				}
			} else {
				auto &list_child = ListType::GetChildType(child_type);
				if (list_child.id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv_auto %s requires a list of types (varchar) as input", kv.first);
				}
				auto &children = ListValue::GetChildren(kv.second);
				for (auto &child : children) {
					sql_type_names.push_back(StringValue::Get(child));
				}
			}
			sql_type_list.reserve(sql_type_names.size());
			for (auto &sql_type : sql_type_names) {
				auto def_type = TransformStringToLogicalType(sql_type, context);
				if (def_type.id() == LogicalTypeId::USER) {
					throw BinderException("Unrecognized type \"%s\" for read_csv_auto %s definition", sql_type,
					                      kv.first);
				}
				sql_type_list.push_back(std::move(def_type));
			}
		} else if (loption == "all_varchar") {
			all_varchar = BooleanValue::Get(kv.second);
		} else if (loption == "normalize_names") {
			normalize_names = BooleanValue::Get(kv.second);
		} else {
			SetReadOption(loption, kv.second, names);
		}
	}
}

//! This function is used to remember options set by the sniffer, for use in ReadCSVRelation
void CSVReaderOptions::ToNamedParameters(named_parameter_map_t &named_params) {
	if (has_delimiter) {
		named_params["delim"] = Value(GetDelimiter());
	}
	if (has_newline) {
		named_params["newline"] = Value(EnumUtil::ToString(GetNewline()));
	}
	if (has_quote) {
		named_params["quote"] = Value(GetQuote());
	}
	if (has_escape) {
		named_params["escape"] = Value(GetEscape());
	}
	if (has_header) {
		named_params["header"] = Value(GetHeader());
	}
	named_params["max_line_size"] = Value::BIGINT(maximum_line_size);
	if (skip_rows_set) {
		named_params["skip"] = Value::BIGINT(GetSkipRows());
	}
	named_params["null_padding"] = Value::BOOLEAN(null_padding);
	if (!date_format.at(LogicalType::DATE).format_specifier.empty()) {
		named_params["dateformat"] = Value(date_format.at(LogicalType::DATE).format_specifier);
	}
	if (!date_format.at(LogicalType::TIMESTAMP).format_specifier.empty()) {
		named_params["timestampformat"] = Value(date_format.at(LogicalType::TIMESTAMP).format_specifier);
	}

	named_params["normalize_names"] = Value::BOOLEAN(normalize_names);
	if (!name_list.empty() && !named_params.count("column_names") && !named_params.count("names")) {
		named_params["column_names"] = StringVectorToValue(name_list);
	}
	named_params["all_varchar"] = Value::BOOLEAN(all_varchar);
	named_params["maximum_line_size"] = Value::BIGINT(maximum_line_size);
}

} // namespace duckdb
