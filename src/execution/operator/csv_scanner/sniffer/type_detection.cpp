#include "duckdb/common/operator/decimal_cast_operators.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
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

struct TupleSniffing {
	idx_t line_number;
	idx_t position;
	bool set = false;
	vector<Value> values;
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
	if (candidate.dialect_options.has_format.find(LogicalTypeId::DATE)->second &&
	    sql_type.id() == LogicalTypeId::DATE) {
		date_t result;
		string error_message;
		return candidate.dialect_options.date_format.find(LogicalTypeId::DATE)
		    ->second.TryParseDate(string_t(StringValue::Get(value)), result, error_message);
	}
	if (candidate.dialect_options.has_format.find(LogicalTypeId::TIMESTAMP)->second &&
	    sql_type.id() == LogicalTypeId::TIMESTAMP) {
		timestamp_t result;
		string error_message;
		return candidate.dialect_options.date_format.find(LogicalTypeId::TIMESTAMP)
		    ->second.TryParseTimestamp(string_t(StringValue::Get(value)), result, error_message);
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
	candidate.dialect_options.has_format[sql_type] = true;
	auto &date_format = candidate.dialect_options.date_format[sql_type];
	date_format.format_specifier = format_specifier;
	StrTimeFormat::ParseFormatSpecifier(date_format.format_specifier, date_format);
}

struct SniffValue {
	inline static void Initialize(CSVStateMachine &machine) {
		machine.state = CSVState::STANDARD;
		machine.previous_state = CSVState::STANDARD;
		machine.pre_previous_state = CSVState::STANDARD;
		machine.cur_rows = 0;
		machine.value = "";
		machine.rows_read = 0;
	}

	inline static bool Process(CSVStateMachine &machine, vector<TupleSniffing> &sniffed_values, char current_char,
	                           idx_t current_pos) {

		if ((machine.dialect_options.new_line == NewLineIdentifier::SINGLE &&
		     (current_char == '\r' || current_char == '\n')) ||
		    (machine.dialect_options.new_line == NewLineIdentifier::CARRY_ON && current_char == '\n')) {
			machine.rows_read++;
			sniffed_values[machine.cur_rows].position = machine.line_start_pos;
			sniffed_values[machine.cur_rows].set = true;
			machine.line_start_pos = current_pos;
		}
		machine.pre_previous_state = machine.previous_state;
		machine.previous_state = machine.state;
		machine.state = static_cast<CSVState>(
		    machine.transition_array[static_cast<uint8_t>(machine.state)][static_cast<uint8_t>(current_char)]);

		bool carriage_return = machine.previous_state == CSVState::CARRIAGE_RETURN;
		if (machine.previous_state == CSVState::DELIMITER ||
		    (machine.previous_state == CSVState::RECORD_SEPARATOR && machine.state != CSVState::EMPTY_LINE) ||
		    (machine.state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8
			machine.VerifyUTF8();
			if (machine.value.empty() || machine.value == machine.options.null_str) {
				// We set empty == null value
				sniffed_values[machine.cur_rows].values.push_back(Value(LogicalType::VARCHAR));
			} else {
				sniffed_values[machine.cur_rows].values.push_back(Value(machine.value));
			}
			sniffed_values[machine.cur_rows].line_number = machine.rows_read;

			machine.value = "";
		}
		if (machine.state == CSVState::STANDARD ||
		    (machine.state == CSVState::QUOTED && machine.previous_state == CSVState::QUOTED)) {
			machine.value += current_char;
		}
		machine.cur_rows +=
		    machine.previous_state == CSVState::RECORD_SEPARATOR && machine.state != CSVState::EMPTY_LINE;
		// It means our carriage return is actually a record separator
		machine.cur_rows += machine.state != CSVState::RECORD_SEPARATOR && carriage_return;
		if (machine.cur_rows >= sniffed_values.size()) {
			// We sniffed enough rows
			return true;
		}
		return false;
	}

	inline static void Finalize(CSVStateMachine &machine, vector<TupleSniffing> &sniffed_values) {
		if (machine.cur_rows < sniffed_values.size() && machine.state == CSVState::DELIMITER) {
			// Started a new empty value
			sniffed_values[machine.cur_rows].values.push_back(Value(machine.value));
		}
		if (machine.cur_rows < sniffed_values.size() && machine.state != CSVState::EMPTY_LINE) {
			machine.VerifyUTF8();
			sniffed_values[machine.cur_rows].line_number = machine.rows_read;
			if (!sniffed_values[machine.cur_rows].set) {
				sniffed_values[machine.cur_rows].position = machine.line_start_pos;
				sniffed_values[machine.cur_rows].set = true;
			}

			sniffed_values[machine.cur_rows++].values.push_back(Value(machine.value));
		}
		sniffed_values.erase(sniffed_values.end() - (sniffed_values.size() - machine.cur_rows), sniffed_values.end());
	}
};

void CSVSniffer::DetectDateAndTimeStampFormats(CSVStateMachine &candidate,
                                               map<LogicalTypeId, bool> &has_format_candidates,
                                               map<LogicalTypeId, vector<string>> &format_candidates,
                                               const LogicalType &sql_type, const string &separator, Value &dummy_val) {
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
		candidate.dialect_options.has_format[sql_type.id()] = true;
		//	all formats are constructed to be valid
		SetDateFormat(candidate, type_format_candidates.back(), sql_type.id());
	}
	// check all formats and keep the first one that works
	StrpTimeFormat::ParseResult result;
	auto save_format_candidates = type_format_candidates;
	while (!type_format_candidates.empty()) {
		//	avoid using exceptions for flow control...
		auto &current_format = candidate.dialect_options.date_format[sql_type.id()];
		if (current_format.Parse(StringValue::Get(dummy_val), result)) {
			break;
		}
		//	doesn't work - move to the next one
		type_format_candidates.pop_back();
		candidate.dialect_options.has_format[sql_type.id()] = (!type_format_candidates.empty());
		if (!type_format_candidates.empty()) {
			SetDateFormat(candidate, type_format_candidates.back(), sql_type.id());
		}
	}
	//	if none match, then this is not a value of type sql_type,
	if (type_format_candidates.empty()) {
		//	so restore the candidates that did work.
		//	or throw them out if they were generated by this value.
		if (had_format_candidates) {
			type_format_candidates.swap(save_format_candidates);
			if (!type_format_candidates.empty()) {
				SetDateFormat(candidate, type_format_candidates.back(), sql_type.id());
			}
		} else {
			has_format_candidates[sql_type.id()] = false;
		}
	}
}

void CSVSniffer::DetectTypes() {
	idx_t min_varchar_cols = max_columns_found + 1;
	vector<LogicalType> return_types;
	// check which info candidate leads to minimum amount of non-varchar columns...
	for (auto &candidate : candidates) {
		unordered_map<idx_t, vector<LogicalType>> info_sql_types_candidates;
		for (idx_t i = 0; i < candidate->dialect_options.num_cols; i++) {
			info_sql_types_candidates[i] = candidate->options.auto_type_candidates;
		}
		map<LogicalTypeId, bool> has_format_candidates;
		map<LogicalTypeId, vector<string>> format_candidates;
		for (const auto &t : format_template_candidates) {
			has_format_candidates[t.first] = false;
			format_candidates[t.first].clear();
		}
		D_ASSERT(candidate->dialect_options.num_cols > 0);

		// Set all return_types to VARCHAR so we can do datatype detection based on VARCHAR values
		return_types.clear();
		return_types.assign(candidate->dialect_options.num_cols, LogicalType::VARCHAR);

		// Reset candidate for parsing
		candidate->Reset();

		// Parse chunk and read csv with info candidate
		idx_t sample_size = options.sample_chunk_size;
		if (options.sample_chunk_size == 1) {
			sample_size++;
		}
		vector<TupleSniffing> tuples(sample_size);
		candidate->csv_buffer_iterator.Process<SniffValue>(*candidate, tuples);
		// Potentially Skip empty rows (I find this dirty, but it is what the original code does)
		idx_t true_start = 0;
		idx_t values_start = 0;
		while (true_start < tuples.size()) {
			if (tuples[true_start].values.empty() ||
			    (tuples[true_start].values.size() == 1 && tuples[true_start].values[0].IsNull())) {
				true_start = tuples[true_start].line_number;
				values_start++;
			} else {
				break;
			}
		}

		// Potentially Skip Notes (I also find this dirty, but it is what the original code does)
		while (true_start < tuples.size()) {
			if (tuples[true_start].values.size() < max_columns_found && !options.null_padding) {
				true_start = tuples[true_start].line_number;
				values_start++;
			} else {
				break;
			}
		}

		tuples.erase(tuples.begin(), tuples.begin() + values_start);
		idx_t row_idx = 0;
		if (tuples.size() > 1 && (!options.has_header || (options.has_header && options.dialect_options.header))) {
			// This means we have more than one row, hence we can use the first row to detect if we have a header
			row_idx = 1;
		}
		if (!tuples.empty()) {
			best_start_without_header = tuples[0].position;
		}

		// First line where we start our type detection
		const idx_t start_idx_detection = row_idx;
		for (; row_idx < tuples.size(); row_idx++) {
			for (idx_t col = 0; col < tuples[row_idx].values.size(); col++) {
				auto &col_type_candidates = info_sql_types_candidates[col];
				auto cur_top_candidate = col_type_candidates.back();
				auto dummy_val = tuples[row_idx].values[col];
				// try cast from string to sql_type
				while (col_type_candidates.size() > 1) {
					const auto &sql_type = col_type_candidates.back();
					// try formatting for date types if the user did not specify one and it starts with numeric values.
					string separator;
					bool has_format_is_set = false;
					auto format_iterator = candidate->dialect_options.has_format.find(sql_type.id());
					if (format_iterator != candidate->dialect_options.has_format.end()) {
						has_format_is_set = format_iterator->second;
					}
					if (has_format_candidates.count(sql_type.id()) &&
					    (!has_format_is_set || format_candidates[sql_type.id()].size() > 1) && !dummy_val.IsNull() &&
					    StartsWithNumericDate(separator, StringValue::Get(dummy_val))) {
						DetectDateAndTimeStampFormats(*candidate, has_format_candidates, format_candidates, sql_type,
						                              separator, dummy_val);
					}
					// try cast from string to sql_type
					if (TryCastValue(*candidate, dummy_val, sql_type)) {
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

		// it's good if the dialect creates more non-varchar columns, but only if we sacrifice < 30% of best_num_cols.
		if (varchar_cols < min_varchar_cols && info_sql_types_candidates.size() > (max_columns_found * 0.7)) {
			// we have a new best_options candidate
			if (true_start > 0) {
				// Add empty rows to skip_rows
				candidate->dialect_options.skip_rows += true_start;
			}
			best_candidate = std::move(candidate);
			min_varchar_cols = varchar_cols;
			best_sql_types_candidates_per_column_idx = info_sql_types_candidates;
			best_format_candidates = format_candidates;
			best_header_row = tuples[0].values;
			best_start_with_header = tuples[0].position;
		}
	}
	// Assert that it's all good at this point.
	D_ASSERT(best_candidate && !best_format_candidates.empty() && !best_header_row.empty());

	for (const auto &best : best_format_candidates) {
		if (!best.second.empty()) {
			SetDateFormat(*best_candidate, best.second.back(), best.first);
		}
	}
}

} // namespace duckdb
