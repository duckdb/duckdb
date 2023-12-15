//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parse_values.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"

namespace duckdb {

struct TupleOfValues {
	idx_t line_number;
	idx_t position;
	bool set = false;
	vector<Value> values;
	void Print(){
		for (auto& val: values){
			val.Print();
		}
	}
};

//! Templated Function that process a CSV buffer into a vector of sniffed values
struct ParseValues {
	inline static void Initialize(CSVScanner &scanner, idx_t cur_pos) {
		scanner.states.Initialize(CSVState::STANDARD);
		scanner.cur_rows = 0;
		scanner.value = "";
		scanner.rows_read = 0;
	}

	inline static bool Process(CSVScanner &scanner, vector<TupleOfValues> &sniffed_values, char current_char,
	                           idx_t current_pos) {
		auto &sniffing_state_machine = scanner.GetStateMachine();
		auto &states = scanner.states;

		//		if ((sniffing_state_machine.dialect_options.new_line == NewLineIdentifier::SINGLE &&
		//		     (current_char == '\r' || current_char == '\n')) ||
		//		    (sniffing_state_machine.dialect_options.new_line == NewLineIdentifier::CARRY_ON && current_char ==
		//'\n')) { 			scanner.rows_read++;
		//		}
		if ((current_char == '\r' || current_char == '\n')) {
			scanner.rows_read++;
		}

		if ((states.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (states.current_state != CSVState::RECORD_SEPARATOR &&
		     states.previous_state == CSVState::CARRIAGE_RETURN)) {
			sniffed_values[scanner.cur_rows].position = scanner.line_start_pos;
			sniffed_values[scanner.cur_rows].set = true;
			scanner.line_start_pos = current_pos;
		}

		sniffing_state_machine.Transition(states, current_char);

		bool carriage_return = states.previous_state == CSVState::CARRIAGE_RETURN;
		if (states.previous_state == CSVState::DELIMITER || (states.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			if (scanner.value.empty() || scanner.value == sniffing_state_machine.options.null_str) {
				// We set empty == null value
				sniffed_values[scanner.cur_rows].values.push_back(Value(LogicalType::VARCHAR));
			} else {
				sniffed_values[scanner.cur_rows].values.push_back(Value(scanner.value));
			}
			sniffed_values[scanner.cur_rows].line_number = scanner.rows_read;

			scanner.value = "";
		}
		if (states.current_state == CSVState::STANDARD ||
		    (states.current_state == CSVState::QUOTED && states.previous_state == CSVState::QUOTED)) {
			scanner.value += current_char;
		}
		scanner.cur_rows += states.previous_state == CSVState::RECORD_SEPARATOR;
		// It means our carriage return is actually a record separator
		scanner.cur_rows += states.current_state != CSVState::RECORD_SEPARATOR && carriage_return;
		if (scanner.cur_rows >= sniffed_values.size()) {
			// We sniffed enough rows
			return true;
		}
		return false;
	}

	inline static void Finalize(CSVScanner &scanner, vector<TupleOfValues> &sniffed_values) {
		if (scanner.cur_rows < sniffed_values.size() && scanner.states.current_state == CSVState::DELIMITER) {
			// Started a new empty value
			sniffed_values[scanner.cur_rows].values.push_back(Value(scanner.value));
		}
		if (scanner.cur_rows < sniffed_values.size() && scanner.states.current_state != CSVState::EMPTY_LINE) {
			sniffed_values[scanner.cur_rows].line_number = scanner.rows_read;
			if (!sniffed_values[scanner.cur_rows].set) {
				sniffed_values[scanner.cur_rows].position = scanner.line_start_pos;
				sniffed_values[scanner.cur_rows].set = true;
			}

			sniffed_values[scanner.cur_rows++].values.push_back(Value(scanner.value));
		}
		sniffed_values.erase(sniffed_values.end() - (sniffed_values.size() - scanner.cur_rows), sniffed_values.end());
	}
};
} // namespace duckdb
