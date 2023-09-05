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

namespace duckdb{

struct TupleOfValues {
	idx_t line_number;
	idx_t position;
	bool set = false;
	vector<Value> values;
};

//! Templated Function that process a CSV buffer into a vector of sniffed values
struct ParseValues {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.state = CSVState::STANDARD;
		scanner.previous_state = CSVState::STANDARD;
		scanner.pre_previous_state = CSVState::STANDARD;
		scanner.cur_rows = 0;
		scanner.value = "";
		scanner.rows_read = 0;
	}

	inline static bool Process(CSVScanner &scanner, vector<TupleOfValues> &sniffed_values, char current_char,
	                           idx_t current_pos) {
		auto &sniffing_state_machine = scanner.GetStateMachineSniff();
		if ((sniffing_state_machine.dialect_options.new_line == NewLineIdentifier::SINGLE &&
		     (current_char == '\r' || current_char == '\n')) ||
		    (sniffing_state_machine.dialect_options.new_line == NewLineIdentifier::CARRY_ON && current_char == '\n')) {
			scanner.rows_read++;
			sniffed_values[scanner.cur_rows].position = scanner.line_start_pos;
			sniffed_values[scanner.cur_rows].set = true;
			scanner.line_start_pos = current_pos;
		}
		scanner.pre_previous_state = scanner.previous_state;
		scanner.previous_state = scanner.state;
		scanner.state = static_cast<CSVState>(
		    sniffing_state_machine
		        .transition_array[static_cast<uint8_t>(scanner.state)][static_cast<uint8_t>(current_char)]);

		bool carriage_return = scanner.previous_state == CSVState::CARRIAGE_RETURN;
		if (scanner.previous_state == CSVState::DELIMITER ||
		    (scanner.previous_state == CSVState::RECORD_SEPARATOR && scanner.state != CSVState::EMPTY_LINE) ||
		    (scanner.state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			// Check if it's UTF-8
			scanner.VerifyUTF8();
			if (scanner.value.empty() || scanner.value == sniffing_state_machine.options.null_str) {
				// We set empty == null value
				sniffed_values[scanner.cur_rows].values.push_back(Value(LogicalType::VARCHAR));
			} else {
				sniffed_values[scanner.cur_rows].values.push_back(Value(scanner.value));
			}
			sniffed_values[scanner.cur_rows].line_number = scanner.rows_read;

			scanner.value = "";
		}
		if (scanner.state == CSVState::STANDARD ||
		    (scanner.state == CSVState::QUOTED && scanner.previous_state == CSVState::QUOTED)) {
			scanner.value += current_char;
		}
		scanner.cur_rows +=
		    scanner.previous_state == CSVState::RECORD_SEPARATOR && scanner.state != CSVState::EMPTY_LINE;
		// It means our carriage return is actually a record separator
		scanner.cur_rows += scanner.state != CSVState::RECORD_SEPARATOR && carriage_return;
		if (scanner.cur_rows >= sniffed_values.size()) {
			// We sniffed enough rows
			return true;
		}
		return false;
	}

	inline static void Finalize(CSVScanner &machine, vector<TupleOfValues> &sniffed_values) {
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
}