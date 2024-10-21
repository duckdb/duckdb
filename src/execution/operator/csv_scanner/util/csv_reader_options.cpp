#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/set.hpp"

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
	return this->dialect_options.header.GetValue();
}

void CSVReaderOptions::SetHeader(bool input) {
	this->dialect_options.header.Set(input);
}

void CSVReaderOptions::SetCompression(const string &compression_p) {
	this->compression = FileCompressionTypeFromString(compression_p);
}

string CSVReaderOptions::GetEscape() const {
	return std::string(1, this->dialect_options.state_machine_options.escape.GetValue());
}

void CSVReaderOptions::SetEscape(const string &input) {
	auto escape_str = input;
	if (escape_str.size() > 1) {
		throw InvalidInputException("The escape option cannot exceed a size of 1 byte.");
	}
	if (escape_str.empty()) {
		escape_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.escape.Set(escape_str[0]);
}

idx_t CSVReaderOptions::GetSkipRows() const {
	return NumericCast<idx_t>(this->dialect_options.skip_rows.GetValue());
}

void CSVReaderOptions::SetSkipRows(int64_t skip_rows) {
	if (skip_rows < 0) {
		throw InvalidInputException("skip_rows option from read_csv scanner, must be equal or higher than 0");
	}
	dialect_options.skip_rows.Set(NumericCast<idx_t>(skip_rows));
}

string CSVReaderOptions::GetDelimiter() const {
	return std::string(1, this->dialect_options.state_machine_options.delimiter.GetValue());
}

void CSVReaderOptions::SetDelimiter(const string &input) {
	auto delim_str = StringUtil::Replace(input, "\\t", "\t");
	if (delim_str.size() > 1) {
		throw InvalidInputException("The delimiter option cannot exceed a size of 1 byte.");
	}
	if (input.empty()) {
		delim_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.delimiter.Set(delim_str[0]);
}

string CSVReaderOptions::GetQuote() const {
	return std::string(1, this->dialect_options.state_machine_options.quote.GetValue());
}

void CSVReaderOptions::SetQuote(const string &quote_p) {
	auto quote_str = quote_p;
	if (quote_str.size() > 1) {
		throw InvalidInputException("The quote option cannot exceed a size of 1 byte.");
	}
	if (quote_str.empty()) {
		quote_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.quote.Set(quote_str[0]);
}

string CSVReaderOptions::GetComment() const {
	return std::string(1, this->dialect_options.state_machine_options.comment.GetValue());
}

void CSVReaderOptions::SetComment(const string &comment_p) {
	auto comment_str = comment_p;
	if (comment_str.size() > 1) {
		throw InvalidInputException("The comment option cannot exceed a size of 1 byte.");
	}
	if (comment_str.empty()) {
		comment_str = string("\0", 1);
	}
	this->dialect_options.state_machine_options.comment.Set(comment_str[0]);
}

string CSVReaderOptions::GetNewline() const {
	switch (dialect_options.state_machine_options.new_line.GetValue()) {
	case NewLineIdentifier::CARRY_ON:
		return "\\r\\n";
	case NewLineIdentifier::SINGLE_R:
		return "\\r";
	case NewLineIdentifier::SINGLE_N:
		return "\\n";
	case NewLineIdentifier::NOT_SET:
		return "";
	default:
		throw NotImplementedException("New line type not supported");
	}
}

void CSVReaderOptions::SetNewline(const string &input) {
	if (input == "\\n") {
		dialect_options.state_machine_options.new_line.Set(NewLineIdentifier::SINGLE_N);
	} else if (input == "\\r") {
		dialect_options.state_machine_options.new_line.Set(NewLineIdentifier::SINGLE_R);
	} else if (input == "\\r\\n") {
		dialect_options.state_machine_options.new_line.Set(NewLineIdentifier::CARRY_ON);
	} else {
		throw InvalidInputException("This is not accepted as a newline: " + input);
	}
}

bool CSVReaderOptions::IgnoreErrors() const {
	return ignore_errors.GetValue() && !store_rejects.GetValue();
}

void CSVReaderOptions::SetDateFormat(LogicalTypeId type, const string &format, bool read_format) {
	string error;
	if (read_format) {
		StrpTimeFormat strpformat;
		error = StrTimeFormat::ParseFormatSpecifier(format, strpformat);
		dialect_options.date_format[type].Set(strpformat);
	} else {
		write_date_format[type] = Value(format);
	}
	if (!error.empty()) {
		throw InvalidInputException("Could not parse DATEFORMAT: %s", error.c_str());
	}
}

void CSVReaderOptions::SetReadOption(const string &loption, const Value &value, vector<string> &expected_names) {
	if (SetBaseOption(loption, value)) {
		return;
	}
	if (loption == "auto_detect") {
		auto_detect = ParseBoolean(value, loption);
	} else if (loption == "sample_size") {
		auto sample_size_option = ParseInteger(value, loption);
		if (sample_size_option < 1 && sample_size_option != -1) {
			throw BinderException("Unsupported parameter for SAMPLE_SIZE: cannot be smaller than 1");
		}
		if (sample_size_option == -1) {
			// If -1, we basically read the whole thing
			sample_size_chunks = NumericLimits<idx_t>().Maximum();
		} else {
			sample_size_chunks = NumericCast<idx_t>(sample_size_option / STANDARD_VECTOR_SIZE);
			if (sample_size_option % STANDARD_VECTOR_SIZE != 0) {
				sample_size_chunks++;
			}
		}

	} else if (loption == "skip") {
		SetSkipRows(ParseInteger(value, loption));
	} else if (loption == "max_line_size" || loption == "maximum_line_size") {
		maximum_line_size = NumericCast<idx_t>(ParseInteger(value, loption));
	} else if (loption == "date_format" || loption == "dateformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::DATE, format, true);
	} else if (loption == "timestamp_format" || loption == "timestampformat") {
		string format = ParseString(value, loption);
		SetDateFormat(LogicalTypeId::TIMESTAMP, format, true);
	} else if (loption == "ignore_errors") {
		ignore_errors.Set(ParseBoolean(value, loption));
	} else if (loption == "buffer_size") {
		buffer_size = NumericCast<idx_t>(ParseInteger(value, loption));
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
	} else if (loption == "parallel") {
		parallel = ParseBoolean(value, loption);
	} else if (loption == "allow_quoted_nulls") {
		allow_quoted_nulls = ParseBoolean(value, loption);
	} else if (loption == "store_rejects") {
		store_rejects.Set(ParseBoolean(value, loption));
	} else if (loption == "force_not_null") {
		if (!expected_names.empty()) {
			force_not_null = ParseColumnList(value, expected_names, loption);
		} else {
			// Get the list of columns to use as a recovery key
			auto &children = ListValue::GetChildren(value);
			for (auto &child : children) {
				auto col_name = child.GetValue<string>();
				force_not_null_names.insert(col_name);
			}
		}

	} else if (loption == "rejects_table") {
		// skip, handled in SetRejectsOptions
		auto table_name = ParseString(value, loption);
		if (table_name.empty()) {
			throw BinderException("REJECTS_TABLE option cannot be empty");
		}
		rejects_table_name.Set(table_name);
	} else if (loption == "rejects_scan") {
		// skip, handled in SetRejectsOptions
		auto table_name = ParseString(value, loption);
		if (table_name.empty()) {
			throw BinderException("rejects_scan option cannot be empty");
		}
		rejects_scan_name.Set(table_name);
	} else if (loption == "rejects_limit") {
		auto limit = ParseInteger(value, loption);
		if (limit < 0) {
			throw BinderException("Unsupported parameter for REJECTS_LIMIT: cannot be negative");
		}
		rejects_limit = NumericCast<idx_t>(limit);
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

	if (SetBaseOption(loption, value, true)) {
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

bool CSVReaderOptions::SetBaseOption(const string &loption, const Value &value, bool write_option) {
	// Make sure this function was only called after the option was turned into lowercase
	D_ASSERT(!std::any_of(loption.begin(), loption.end(), ::isupper));

	if (StringUtil::StartsWith(loption, "delim") || StringUtil::StartsWith(loption, "sep")) {
		SetDelimiter(ParseString(value, loption));
	} else if (loption == "quote") {
		SetQuote(ParseString(value, loption));
	} else if (loption == "comment") {
		SetComment(ParseString(value, loption));
	} else if (loption == "new_line") {
		SetNewline(ParseString(value, loption));
	} else if (loption == "escape") {
		SetEscape(ParseString(value, loption));
	} else if (loption == "header") {
		SetHeader(ParseBoolean(value, loption));
	} else if (loption == "nullstr" || loption == "null") {
		auto &child_type = value.type();
		null_str.clear();
		if (child_type.id() != LogicalTypeId::LIST && child_type.id() != LogicalTypeId::VARCHAR) {
			throw BinderException("CSV Reader function option %s requires a string or a list as input", loption);
		}
		if (!null_str.empty()) {
			throw BinderException("CSV Reader function option nullstr can only be supplied once");
		}
		if (child_type.id() == LogicalTypeId::LIST) {
			auto &list_child = ListType::GetChildType(child_type);
			const vector<Value> *children = nullptr;
			if (list_child.id() == LogicalTypeId::LIST) {
				// This can happen if it comes from a copy FROM/TO
				auto &list_grandchild = ListType::GetChildType(list_child);
				auto &children_ref = ListValue::GetChildren(value);
				if (list_grandchild.id() != LogicalTypeId::VARCHAR || children_ref.size() != 1) {
					throw BinderException("CSV Reader function option %s requires a non-empty list of possible null "
					                      "strings (varchar) as input",
					                      loption);
				}
				children = &ListValue::GetChildren(children_ref.back());
			} else if (list_child.id() != LogicalTypeId::VARCHAR) {
				throw BinderException("CSV Reader function option %s requires a non-empty list of possible null "
				                      "strings (varchar) as input",
				                      loption);
			}
			if (!children) {
				children = &ListValue::GetChildren(value);
			}
			for (auto &child : *children) {
				if (child.IsNull()) {
					throw BinderException(
					    "CSV Reader function option %s does not accept NULL values as a valid nullstr option", loption);
				}
				null_str.push_back(StringValue::Get(child));
			}
		} else {
			null_str.push_back(StringValue::Get(ParseString(value, loption)));
		}
		if (null_str.size() > 1 && write_option) {
			throw BinderException("CSV Writer function option %s only accepts one nullstr value.", loption);
		}

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

template <class T>
string FormatOptionLine(const string &name, const CSVOption<T> option) {
	return name + " = " + option.FormatValue() + " " + option.FormatSet() + "\n  ";
}
bool CSVReaderOptions::WasTypeManuallySet(idx_t i) const {
	if (i >= was_type_manually_set.size()) {
		return false;
	}
	return was_type_manually_set[i];
}

string CSVReaderOptions::ToString(const string &current_file_path) const {
	auto &delimiter = dialect_options.state_machine_options.delimiter;
	auto &quote = dialect_options.state_machine_options.quote;
	auto &escape = dialect_options.state_machine_options.escape;
	auto &comment = dialect_options.state_machine_options.comment;
	auto &new_line = dialect_options.state_machine_options.new_line;
	auto &skip_rows = dialect_options.skip_rows;

	auto &header = dialect_options.header;
	string error = "  file = " + current_file_path + "\n  ";
	// Let's first print options that can either be set by the user or by the sniffer
	// delimiter
	error += FormatOptionLine("delimiter", delimiter);
	// quote
	error += FormatOptionLine("quote", quote);
	// escape
	error += FormatOptionLine("escape", escape);
	// newline
	error += FormatOptionLine("new_line", new_line);
	// has_header
	error += FormatOptionLine("header", header);
	// skip_rows
	error += FormatOptionLine("skip_rows", skip_rows);
	// comment
	error += FormatOptionLine("comment", comment);
	// date format
	error += FormatOptionLine("date_format", dialect_options.date_format.at(LogicalType::DATE));
	// timestamp format
	error += FormatOptionLine("timestamp_format", dialect_options.date_format.at(LogicalType::TIMESTAMP));

	// Now we do options that can only be set by the user, that might hold some general significance
	// null padding
	error += "null_padding = " + std::to_string(null_padding) + "\n  ";
	// sample_size
	error += "sample_size = " + std::to_string(sample_size_chunks * STANDARD_VECTOR_SIZE) + "\n  ";
	// ignore_errors
	error += "ignore_errors = " + ignore_errors.FormatValue() + "\n  ";
	// all_varchar
	error += "all_varchar = " + std::to_string(all_varchar) + "\n";

	// Add information regarding sniffer mismatches (if any)
	error += sniffer_user_mismatch_error;
	return error;
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
	    {static_cast<uint8_t>(LogicalTypeId::VARCHAR), 0},   {static_cast<uint8_t>(LogicalTypeId::DOUBLE), 1},
	    {static_cast<uint8_t>(LogicalTypeId::FLOAT), 2},     {static_cast<uint8_t>(LogicalTypeId::DECIMAL), 3},
	    {static_cast<uint8_t>(LogicalTypeId::BIGINT), 4},    {static_cast<uint8_t>(LogicalTypeId::INTEGER), 5},
	    {static_cast<uint8_t>(LogicalTypeId::SMALLINT), 6},  {static_cast<uint8_t>(LogicalTypeId::TINYINT), 7},
	    {static_cast<uint8_t>(LogicalTypeId::TIMESTAMP), 8}, {static_cast<uint8_t>(LogicalTypeId::DATE), 9},
	    {static_cast<uint8_t>(LogicalTypeId::TIME), 10},     {static_cast<uint8_t>(LogicalTypeId::BOOLEAN), 11},
	    {static_cast<uint8_t>(LogicalTypeId::SQLNULL), 12}};

	auto id = static_cast<uint8_t>(candidate_type.id());
	auto it = auto_type_candidates_specificity.find(id);
	if (it == auto_type_candidates_specificity.end()) {
		throw BinderException("Auto Type Candidate of type %s is not accepted as a valid input",
		                      EnumUtil::ToString(candidate_type.id()));
	}
	return it->second;
}
bool StoreUserDefinedParameter(const string &option) {
	if (option == "column_types" || option == "types" || option == "dtypes" || option == "auto_detect" ||
	    option == "auto_type_candidates" || option == "columns" || option == "names") {
		// We don't store options related to types, names and auto-detection since these are either irrelevant to our
		// prompt or are covered by the columns option.
		return false;
	}
	return true;
}

void CSVReaderOptions::Verify() {
	if (rejects_table_name.IsSetByUser() && !store_rejects.GetValue() && store_rejects.IsSetByUser()) {
		throw BinderException("REJECTS_TABLE option is only supported when store_rejects is not manually set to false");
	}
	if (rejects_scan_name.IsSetByUser() && !store_rejects.GetValue() && store_rejects.IsSetByUser()) {
		throw BinderException("REJECTS_SCAN option is only supported when store_rejects is not manually set to false");
	}
	if (rejects_scan_name.IsSetByUser() || rejects_table_name.IsSetByUser()) {
		// Ensure we set store_rejects to true automagically
		store_rejects.Set(true, false);
	}
	// Validate rejects_table options
	if (store_rejects.GetValue()) {
		if (!ignore_errors.GetValue() && ignore_errors.IsSetByUser()) {
			throw BinderException(
			    "STORE_REJECTS option is only supported when IGNORE_ERRORS is not manually set to false");
		}
		// Ensure we set ignore errors to true automagically
		ignore_errors.Set(true, false);
		if (file_options.union_by_name) {
			throw BinderException("REJECTS_TABLE option is not supported when UNION_BY_NAME is set to true");
		}
	}
	if (rejects_limit != 0 && !store_rejects.GetValue()) {
		throw BinderException("REJECTS_LIMIT option is only supported when REJECTS_TABLE is set to a table name");
	}
}

void CSVReaderOptions::FromNamedParameters(const named_parameter_map_t &in, ClientContext &context) {
	map<string, string> ordered_user_defined_parameters;
	for (auto &kv : in) {
		if (MultiFileReader().ParseOption(kv.first, kv.second, file_options, context)) {
			continue;
		}
		auto loption = StringUtil::Lower(kv.first);
		// skip variables that are specific to auto-detection
		if (StoreUserDefinedParameter(loption)) {
			ordered_user_defined_parameters[loption] = kv.second.ToSQLString();
		}
		if (loption == "columns") {
			if (!name_list.empty()) {
				throw BinderException("read_csv column_names/names can only be supplied once");
			}
			columns_set = true;
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT) {
				throw BinderException("read_csv columns requires a struct as input");
			}
			auto &struct_children = StructValue::GetChildren(kv.second);
			D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				auto &name = StructType::GetChildName(child_type, i);
				auto &val = struct_children[i];
				name_list.push_back(name);
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv requires a type specification as string");
				}
				sql_types_per_column[name] = i;
				sql_type_list.emplace_back(TransformStringToLogicalType(StringValue::Get(val), context));
			}
			if (name_list.empty()) {
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
			unordered_set<string> column_names;
			if (!name_list.empty()) {
				throw BinderException("read_csv column_names/names can only be supplied once");
			}
			if (kv.second.IsNull()) {
				throw BinderException("read_csv %s cannot be NULL", kv.first);
			}
			auto &children = ListValue::GetChildren(kv.second);
			for (auto &child : children) {
				name_list.push_back(StringValue::Get(child));
			}
			for (auto &name : name_list) {
				bool empty = true;
				for (auto &c : name) {
					if (!StringUtil::CharacterIsSpace(c)) {
						empty = false;
						break;
					}
				}
				if (empty) {
					throw BinderException("read_csv %s cannot have empty (or all whitespace) value", kv.first);
				}
				if (column_names.find(name) != column_names.end()) {
					throw BinderException("read_csv %s must have unique values. \"%s\" is repeated.", kv.first, name);
				}
				column_names.insert(name);
			}
		} else if (loption == "column_types" || loption == "types" || loption == "dtypes") {
			auto &child_type = kv.second.type();
			if (child_type.id() != LogicalTypeId::STRUCT && child_type.id() != LogicalTypeId::LIST) {
				throw BinderException("read_csv %s requires a struct or list as input", kv.first);
			}
			if (!sql_type_list.empty()) {
				throw BinderException("read_csv column_types/types/dtypes can only be supplied once");
			}
			vector<string> sql_type_names;
			if (child_type.id() == LogicalTypeId::STRUCT) {
				auto &struct_children = StructValue::GetChildren(kv.second);
				D_ASSERT(StructType::GetChildCount(child_type) == struct_children.size());
				for (idx_t i = 0; i < struct_children.size(); i++) {
					auto &name = StructType::GetChildName(child_type, i);
					auto &val = struct_children[i];
					if (val.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("read_csv %s requires a type specification as string", kv.first);
					}
					sql_type_names.push_back(StringValue::Get(val));
					sql_types_per_column[name] = i;
				}
			} else {
				auto &list_child = ListType::GetChildType(child_type);
				if (list_child.id() != LogicalTypeId::VARCHAR) {
					throw BinderException("read_csv %s requires a list of types (varchar) as input", kv.first);
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
					throw BinderException("Unrecognized type \"%s\" for read_csv %s definition", sql_type, kv.first);
				}
				sql_type_list.push_back(std::move(def_type));
			}
		} else if (loption == "all_varchar") {
			all_varchar = BooleanValue::Get(kv.second);
		} else if (loption == "normalize_names") {
			normalize_names = BooleanValue::Get(kv.second);
		} else {
			SetReadOption(loption, kv.second, name_list);
		}
	}
	for (auto &udf_parameter : ordered_user_defined_parameters) {
		user_defined_parameters += udf_parameter.first + "=" + udf_parameter.second + ", ";
	}
	if (user_defined_parameters.size() >= 2) {
		user_defined_parameters.erase(user_defined_parameters.size() - 2);
	}
}

//! This function is used to remember options set by the sniffer, for use in ReadCSVRelation
void CSVReaderOptions::ToNamedParameters(named_parameter_map_t &named_params) const {
	auto &delimiter = dialect_options.state_machine_options.delimiter;
	auto &quote = dialect_options.state_machine_options.quote;
	auto &escape = dialect_options.state_machine_options.escape;
	auto &comment = dialect_options.state_machine_options.comment;
	auto &header = dialect_options.header;
	if (delimiter.IsSetByUser()) {
		named_params["delim"] = Value(GetDelimiter());
	}
	if (dialect_options.state_machine_options.new_line.IsSetByUser()) {
		named_params["new_line"] = Value(GetNewline());
	}
	if (quote.IsSetByUser()) {
		named_params["quote"] = Value(GetQuote());
	}
	if (escape.IsSetByUser()) {
		named_params["escape"] = Value(GetEscape());
	}
	if (comment.IsSetByUser()) {
		named_params["comment"] = Value(GetComment());
	}
	if (header.IsSetByUser()) {
		named_params["header"] = Value(GetHeader());
	}
	named_params["max_line_size"] = Value::BIGINT(NumericCast<int64_t>(maximum_line_size));
	if (dialect_options.skip_rows.IsSetByUser()) {
		named_params["skip"] = Value::UBIGINT(GetSkipRows());
	}
	named_params["null_padding"] = Value::BOOLEAN(null_padding);
	named_params["parallel"] = Value::BOOLEAN(parallel);
	if (!dialect_options.date_format.at(LogicalType::DATE).GetValue().format_specifier.empty()) {
		named_params["dateformat"] =
		    Value(dialect_options.date_format.at(LogicalType::DATE).GetValue().format_specifier);
	}
	if (!dialect_options.date_format.at(LogicalType::TIMESTAMP).GetValue().format_specifier.empty()) {
		named_params["timestampformat"] =
		    Value(dialect_options.date_format.at(LogicalType::TIMESTAMP).GetValue().format_specifier);
	}

	named_params["normalize_names"] = Value::BOOLEAN(normalize_names);
	if (!name_list.empty() && !named_params.count("columns") && !named_params.count("column_names") &&
	    !named_params.count("names")) {
		named_params["column_names"] = StringVectorToValue(name_list);
	}
	named_params["all_varchar"] = Value::BOOLEAN(all_varchar);
	named_params["maximum_line_size"] = Value::BIGINT(NumericCast<int64_t>(maximum_line_size));
}

} // namespace duckdb
