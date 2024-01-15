#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
struct TryCastFloatingOperator {
	template <class OP, class T>
	static bool Operation(string_t input) {
		T result;
		string error_message;
		return OP::Operation(input, result, &error_message);
	}
};

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
	// All our date formats must have at least one -
	D_ASSERT(amount_of_dashes);
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

bool CSVSniffer::TryCastValue(CSVStateMachine &candidate, const Value &value, const LogicalType &sql_type) {
	if (value.IsNull()) {
		return true;
	}
	if (!candidate.dialect_options.date_format.find(LogicalTypeId::DATE)->second.GetValue().Empty() &&
	    sql_type.id() == LogicalTypeId::DATE) {
		date_t result;
		string error_message;
		return candidate.dialect_options.date_format.find(LogicalTypeId::DATE)
		    ->second.GetValue()
		    .TryParseDate(string_t(StringValue::Get(value)), result, error_message);
	}
	if (!candidate.dialect_options.date_format.find(LogicalTypeId::TIMESTAMP)->second.GetValue().Empty() &&
	    sql_type.id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t result;
		string error_message;
		return candidate.dialect_options.date_format.find(LogicalTypeId::TIMESTAMP)
		    ->second.GetValue()
		    .TryParseTimestamp(string_t(StringValue::Get(value)), result, error_message);
	}
	if (candidate.options.decimal_separator != "." && (sql_type.id() == LogicalTypeId::DOUBLE)) {
		return TryCastFloatingOperator::Operation<TryCastErrorMessageCommaSeparated, double>(StringValue::Get(value));
	}
	Value new_value;
	string error_message;
	return value.TryCastAs(buffer_manager->context, sql_type, new_value, &error_message, true);
}

void CSVSniffer::SetDateFormat(CSVStateMachine &candidate, const string &format_specifier,
                               const LogicalTypeId &sql_type) {
	StrpTimeFormat strpformat;
	StrTimeFormat::ParseFormatSpecifier(format_specifier, strpformat);
	candidate.dialect_options.date_format[sql_type].Set(strpformat, false);
}

void CSVSniffer::InitializeDateAndTimeStampDetection(CSVStateMachine &candidate, const string &separator,
                                                     const LogicalType &sql_type) {
	auto &format_candidate = format_candidates[sql_type.id()];
	if (!format_candidate.initialized) {
		format_candidate.initialized = true;
		// order by preference
		auto entry = format_template_candidates.find(sql_type.id());
		if (entry != format_template_candidates.end()) {
			const auto &format_template_list = entry->second;
			for (const auto &t : format_template_list) {
				const auto format_string = GenerateDateFormat(separator, t);
				// don't parse ISO 8601
				if (format_string.find("%Y-%m-%d") == string::npos) {
					format_candidate.format.emplace_back(format_string);
				}
			}
		}
	}
	//	initialise the first candidate
	//	all formats are constructed to be valid
	SetDateFormat(candidate, format_candidate.format.back(), sql_type.id());
}

void CSVSniffer::DetectDateAndTimeStampFormats(CSVStateMachine &candidate, const LogicalType &sql_type,
                                               const string &separator, Value &dummy_val) {
	// If it is the first time running date/timestamp detection we must initilize the format variables
	InitializeDateAndTimeStampDetection(candidate, separator, sql_type);
	// generate date format candidates the first time through
	auto &type_format_candidates = format_candidates[sql_type.id()].format;
	// check all formats and keep the first one that works
	StrpTimeFormat::ParseResult result;
	auto save_format_candidates = type_format_candidates;
	bool had_format_candidates = !save_format_candidates.empty();
	bool initial_format_candidates =
	    save_format_candidates.size() == format_template_candidates.at(sql_type.id()).size();
	while (!type_format_candidates.empty()) {
		//	avoid using exceptions for flow control...
		auto &current_format = candidate.dialect_options.date_format[sql_type.id()].GetValue();
		if (current_format.Parse(StringValue::Get(dummy_val), result)) {
			break;
		}
		//	doesn't work - move to the next one
		type_format_candidates.pop_back();
		if (!type_format_candidates.empty()) {
			SetDateFormat(candidate, type_format_candidates.back(), sql_type.id());
		}
	}
	//	if none match, then this is not a value of type sql_type,
	if (type_format_candidates.empty()) {
		//	so restore the candidates that did work.
		//	or throw them out if they were generated by this value.
		if (had_format_candidates) {
			if (initial_format_candidates) {
				// we reset the whole thing because we tried to sniff the wrong type.
				format_candidates[sql_type.id()].initialized = false;
				format_candidates[sql_type.id()].format.clear();
				return;
			}
			type_format_candidates.swap(save_format_candidates);
			SetDateFormat(candidate, "", sql_type.id());
		}
	}
}

void CSVSniffer::DetectTypes() {
	idx_t min_varchar_cols = max_columns_found + 1;
	vector<LogicalType> return_types;
	// check which info candidate leads to minimum amount of non-varchar columns...
	for (auto &candidate_cc : candidates) {
		auto &sniffing_state_machine = candidate_cc->GetStateMachine();
		unordered_map<idx_t, vector<LogicalType>> info_sql_types_candidates;
		for (idx_t i = 0; i < max_columns_found; i++) {
			info_sql_types_candidates[i] = sniffing_state_machine.options.auto_type_candidates;
		}
		D_ASSERT(max_columns_found > 0);

		// Set all return_types to VARCHAR, so we can do datatype detection based on VARCHAR values
		return_types.clear();
		return_types.assign(max_columns_found, LogicalType::VARCHAR);

		// Reset candidate for parsing
		auto candidate = candidate_cc->UpgradeToStringValueScanner();

		// Parse chunk and read csv with info candidate
		auto &tuples = candidate->ParseChunk();
		idx_t row_idx = 0;
		if (tuples.NumberOfRows() > 1 &&
		    (!options.dialect_options.header.IsSetByUser() ||
		     (options.dialect_options.header.IsSetByUser() && options.dialect_options.header.GetValue()))) {
			// This means we have more than one row, hence we can use the first row to detect if we have a header
			row_idx = 1;
		}
		// First line where we start our type detection
		const idx_t start_idx_detection = row_idx;
		for (; row_idx < tuples.NumberOfRows(); row_idx++) {
			for (idx_t col_idx = 0; col_idx < tuples.number_of_columns; col_idx++) {
				auto &col_type_candidates = info_sql_types_candidates[col_idx];
				// col_type_candidates can't be empty since anything in a CSV file should at least be a string
				// and we validate utf-8 compatibility when creating the type
				D_ASSERT(!col_type_candidates.empty());
				auto cur_top_candidate = col_type_candidates.back();
				auto dummy_val = tuples.GetValue(row_idx, col_idx);
				// try cast from string to sql_type
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					// try formatting for date types if the user did not specify one and it starts with numeric
					// values.
					string separator;
					// If Value is not Null, Has a numeric date format, and the current investigated candidate is
					// either a timestamp or a date
					if (!dummy_val.IsNull() && StartsWithNumericDate(separator, StringValue::Get(dummy_val)) &&
					    (col_type_candidates.back().id() == LogicalTypeId::TIMESTAMP ||
					     col_type_candidates.back().id() == LogicalTypeId::DATE)) {
						DetectDateAndTimeStampFormats(candidate->GetStateMachine(), sql_type, separator, dummy_val);
					}
					// try cast from string to sql_type
					if (sql_type == LogicalType::VARCHAR) {
						// Nothing to convert it to
						continue;
					}
					if (TryCastValue(sniffing_state_machine, dummy_val, sql_type)) {
						break;
					} else {
						if (row_idx != start_idx_detection && cur_top_candidate == LogicalType::BOOLEAN) {
							// If we thought this was a boolean value (i.e., T,F, True, False) and it is not, we
							// immediately pop to varchar.
							while (col_type_candidates.back() != LogicalType::VARCHAR) {
								col_type_candidates.pop_back();
							}
							break;
						}
						col_type_candidates.pop_back();
					}
				}
			}
		}

		idx_t varchar_cols = 0;

		for (idx_t col = 0; col < info_sql_types_candidates.size(); col++) {
			auto &col_type_candidates = info_sql_types_candidates[col];
			// check number of varchar columns
			const auto &col_type = col_type_candidates.back();
			if (col_type == LogicalType::VARCHAR) {
				varchar_cols++;
			}
		}

		// it's good if the dialect creates more non-varchar columns, but only if we sacrifice < 30% of
		// best_num_cols.
		if (varchar_cols < min_varchar_cols && info_sql_types_candidates.size() > (max_columns_found * 0.7)) {
			best_header_row.clear();
			// we have a new best_options candidate
			best_candidate = std::move(candidate);
			min_varchar_cols = varchar_cols;
			best_sql_types_candidates_per_column_idx = info_sql_types_candidates;
			for (auto &format_candidate : format_candidates) {
				best_format_candidates[format_candidate.first] = format_candidate.second.format;
			}
			for (idx_t col_idx = 0; col_idx < tuples.number_of_columns; col_idx++) {
				best_header_row.emplace_back(tuples.GetValue(0, col_idx));
			}
		}
	}
	if (!best_candidate) {
		auto error = CSVError::SniffingError(options.file_path);
		error_handler->Error(error);
	}
	// Assert that it's all good at this point.
	D_ASSERT(best_candidate && !best_format_candidates.empty() && !best_header_row.empty());
}

} // namespace duckdb
