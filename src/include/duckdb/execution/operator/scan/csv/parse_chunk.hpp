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
		scanner.state = CSVState::STANDARD;
		scanner.previous_state = CSVState::STANDARD;
		scanner.pre_previous_state = CSVState::STANDARD;

		scanner.cur_rows = 0;
		scanner.column_count = 0;
		scanner.value = "";
	}

	inline static bool Process(CSVScanner &scanner, DataChunk &parse_chunk, char current_char, idx_t current_pos) {
		auto &sniffing_state_machine = scanner.GetStateMachineSniff();
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
			// Check if it's UTF-8 (Or not?)
			scanner.VerifyUTF8();
			auto &v = parse_chunk.data[scanner.column_count++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			auto &validity_mask = FlatVector::Validity(v);
			if (scanner.value.empty()) {
				validity_mask.SetInvalid(scanner.cur_rows);
			} else {
				parse_data[scanner.cur_rows] = StringVector::AddStringOrBlob(v, string_t(scanner.value));
			}
			scanner.value = "";
		}
		if (((scanner.previous_state == CSVState::RECORD_SEPARATOR && scanner.state != CSVState::EMPTY_LINE) ||
		     (scanner.state != CSVState::RECORD_SEPARATOR && carriage_return)) &&
		    sniffing_state_machine.options.null_padding && scanner.column_count < parse_chunk.ColumnCount()) {
			// It's a new row, check if we need to pad stuff
			while (scanner.column_count < parse_chunk.ColumnCount()) {
				auto &v = parse_chunk.data[scanner.column_count++];
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(scanner.cur_rows);
			}
		}
		if (scanner.state == CSVState::STANDARD ||
		    (scanner.state == CSVState::QUOTED && scanner.previous_state == CSVState::QUOTED)) {
			scanner.value += current_char;
		}
		scanner.cur_rows +=
		    scanner.previous_state == CSVState::RECORD_SEPARATOR && scanner.state != CSVState::EMPTY_LINE;
		scanner.column_count -= scanner.column_count * (scanner.previous_state == CSVState::RECORD_SEPARATOR);

		// It means our carriage return is actually a record separator
		scanner.cur_rows += scanner.state != CSVState::RECORD_SEPARATOR && carriage_return;
		scanner.column_count -= scanner.column_count * (scanner.state != CSVState::RECORD_SEPARATOR && carriage_return);

		if (scanner.cur_rows >= STANDARD_VECTOR_SIZE) {
			// We sniffed enough rows
			return true;
		}
		return false;
	}

	inline static void Finalize(CSVScanner &scanner, DataChunk &parse_chunk) {
		if (scanner.cur_rows < STANDARD_VECTOR_SIZE && scanner.state != CSVState::EMPTY_LINE) {
			scanner.VerifyUTF8();
			auto &v = parse_chunk.data[scanner.column_count++];
			auto parse_data = FlatVector::GetData<string_t>(v);
			if (scanner.value.empty()) {
				auto &validity_mask = FlatVector::Validity(v);
				validity_mask.SetInvalid(scanner.cur_rows);
			} else {
				parse_data[scanner.cur_rows] = StringVector::AddStringOrBlob(v, string_t(scanner.value));
			}
			while (scanner.column_count < parse_chunk.ColumnCount()) {
				auto &v_pad = parse_chunk.data[scanner.column_count++];
				auto &validity_mask = FlatVector::Validity(v_pad);
				validity_mask.SetInvalid(scanner.cur_rows);
			}
			scanner.cur_rows++;
		}
		parse_chunk.SetCardinality(scanner.cur_rows);
	}
}
};
}
