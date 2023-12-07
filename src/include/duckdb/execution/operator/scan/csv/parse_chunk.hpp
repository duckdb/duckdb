//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parse_chunk.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/operator/scan/csv/csv_scanner.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"

namespace duckdb {
struct ParseChunk {
	inline static void Initialize(CSVScanner &scanner) {
		scanner.states.Initialize(CSVState::EMPTY_LINE);
		scanner.cur_rows = 0;
		scanner.column_count = 0;
		scanner.value_pos = scanner.cur_buffer_handle->start_position;
		scanner.length = 0;
	}

	// break it into a 2-step
	// 1 get all positions
	// 2 construct all varchars
	inline static bool Process(CSVScanner &scanner, DataChunk &parse_chunk, char current_char, idx_t current_pos) {
		auto &sniffing_state_machine = scanner.GetStateMachineSniff();
		auto &states = scanner.states;

		sniffing_state_machine.Transition(states, current_char);

		bool carriage_return = states.previous_state == CSVState::CARRIAGE_RETURN;
		if (states.previous_state == CSVState::DELIMITER || (states.previous_state == CSVState::RECORD_SEPARATOR) ||
		    (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return)) {
			// Started a new value
			if (scanner.column_count >= parse_chunk.ColumnCount() && sniffing_state_machine.options.ignore_errors) {
				return false;
			}
			if (scanner.column_count >= parse_chunk.ColumnCount()) {
				throw InvalidInputException("Error in file \"%s\": CSV options could not be auto-detected. Consider "
				                            "setting parser options manually.",
				                            sniffing_state_machine.options.file_path);
			}
			auto &v = parse_chunk.data[scanner.column_count++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			if (scanner.length == 0) {
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(scanner.cur_rows);
			} else {
				parse_data[scanner.cur_rows] = StringVector::AddStringOrBlob(v, string_t(&scanner.cur_buffer_handle->Ptr()[scanner.value_pos], scanner.length));
			}
			scanner.value_pos = current_pos;
			scanner.length = 0;
		}
		if (((states.previous_state == CSVState::RECORD_SEPARATOR && states.current_state != CSVState::EMPTY_LINE) ||
		     (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return)) &&
		    (sniffing_state_machine.options.null_padding || sniffing_state_machine.options.ignore_errors) &&
		    scanner.column_count < parse_chunk.ColumnCount()) {
			// It's a new row, check if we need to pad stuff
			while (scanner.column_count < parse_chunk.ColumnCount()) {
				auto &v = parse_chunk.data[scanner.column_count++];
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(scanner.cur_rows);
			}
		}
		if (states.current_state == CSVState::STANDARD ||
		    (states.current_state == CSVState::QUOTED && states.previous_state == CSVState::QUOTED)) {
			scanner.length ++;
		}
		scanner.cur_rows += states.previous_state == CSVState::RECORD_SEPARATOR && scanner.column_count > 0;
		scanner.column_count -= scanner.column_count * (states.previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		scanner.cur_rows +=
		    states.current_state != CSVState::RECORD_SEPARATOR && carriage_return && scanner.column_count > 0;
		scanner.column_count -=
		    scanner.column_count * (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return);

		if (scanner.cur_rows >= STANDARD_VECTOR_SIZE) {
			// We parsed enough rows
			return true;
		}
		return false;
	}

	inline static void Finalize(CSVScanner &scanner, DataChunk &parse_chunk) {
		auto &sniffing_state_machine = scanner.GetStateMachineSniff();
		auto &states = scanner.states;

		if (scanner.cur_rows < STANDARD_VECTOR_SIZE && states.current_state != CSVState::EMPTY_LINE && scanner.Last()) {
			if (scanner.column_count < parse_chunk.ColumnCount() || !sniffing_state_machine.options.ignore_errors) {
				auto &v = parse_chunk.data[scanner.column_count++];
				auto parse_data = FlatVector::GetData<string_t>(v);
				if (scanner.length == 0) {
					auto &validity_mask = FlatVector::Validity(v);
					validity_mask.SetInvalid(scanner.cur_rows);
				} else {
					parse_data[scanner.cur_rows] = StringVector::AddStringOrBlob(v, string_t(&scanner.cur_buffer_handle->Ptr()[scanner.value_pos], scanner.length));
				}
				while (scanner.column_count < parse_chunk.ColumnCount()) {
					auto &v_pad = parse_chunk.data[scanner.column_count++];
					auto &validity_mask = FlatVector::Validity(v_pad);
					validity_mask.SetInvalid(scanner.cur_rows);
				}
			}

			scanner.cur_rows++;
		}
		parse_chunk.SetCardinality(scanner.cur_rows);
	}
};
} // namespace duckdb
