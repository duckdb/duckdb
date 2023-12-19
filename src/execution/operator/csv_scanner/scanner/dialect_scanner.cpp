#include "duckdb/execution/operator/scan/csv/scanner/dialect_scanner.hpp"

namespace duckdb {
DialectScanner::DialectScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
                               ScannerBoundary boundary)
    : BaseScanner(buffer_manager, state_machine, boundary), column_count(1) {
	result.current_rows = 0;
};

DialectResult *DialectScanner::ParseChunk() {
	result.current_rows = 0;
	column_count = 1;
	ParseChunkInternal();
	return &result;
}
void DialectScanner::Initialize() {
	states.Initialize(CSVState::EMPTY_LINE);
}

void DialectScanner::Process() {
	//! FIXME write functions that get the current char
	char current_char = 'b';
	if (states.current_state == CSVState::INVALID) {
		sniffed_column_counts.clear();
		return true;
	}
	sniffing_state_machine.Transition(states, current_char);

	bool carriage_return = states.previous_state == CSVState::CARRIAGE_RETURN;
	scanner.column_count += states.previous_state == CSVState::DELIMITER;
	sniffed_column_counts[scanner.cur_rows] = scanner.column_count;
	scanner.cur_rows +=
	    states.previous_state == CSVState::RECORD_SEPARATOR && states.current_state != CSVState::EMPTY_LINE;
	scanner.column_count -= (scanner.column_count - 1) * (states.previous_state == CSVState::RECORD_SEPARATOR);

	// It means our carriage return is actually a record separator
	scanner.cur_rows += states.current_state != CSVState::RECORD_SEPARATOR && carriage_return;
	scanner.column_count -=
	    (scanner.column_count - 1) * (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return);

	// Identify what is our line separator
	sniffing_state_machine.carry_on_separator =
	    (states.current_state == CSVState::RECORD_SEPARATOR && carriage_return) ||
	    sniffing_state_machine.carry_on_separator;
	sniffing_state_machine.single_record_separator =
	    ((states.current_state != CSVState::RECORD_SEPARATOR && carriage_return) ||
	     (states.current_state == CSVState::RECORD_SEPARATOR && !carriage_return)) ||
	    sniffing_state_machine.single_record_separator;
	if (scanner.cur_rows >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
	return false;
}

void DialectScanner::FinalizeChunkProcess() {
}
} // namespace duckdb
