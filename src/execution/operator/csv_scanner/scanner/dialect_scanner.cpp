#include "duckdb/execution/operator/scan/csv/scanner/dialect_scanner.hpp"

namespace duckdb {
DialectScanner::DialectScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine), column_count(1) {
	result.cur_rows = 0;
};

DialectResult *DialectScanner::ParseChunk() {
	result.cur_rows = 0;
	column_count = 1;
	ParseChunkInternal();
	return &result;
}
void DialectScanner::Initialize() {
	states.Initialize(CSVState::EMPTY_LINE);
}

bool DialectScanner::ProcessInternal(char current_char) {
	if (states.current_state == CSVState::INVALID) {
		result.cur_rows = 0;
		return true;
	}
	state_machine->Transition(states, current_char);

	bool carriage_return = states.previous_state == CSVState::CARRIAGE_RETURN;
	column_count += states.previous_state == CSVState::DELIMITER;
	result.sniffed_column_counts[result.cur_rows] = column_count;
	result.cur_rows +=
	    states.previous_state == CSVState::RECORD_SEPARATOR && states.current_state != CSVState::EMPTY_LINE;
	column_count -= (column_count - 1) * (states.previous_state == CSVState::RECORD_SEPARATOR);

	// It means our carriage return is actually a record separator
	result.cur_rows += states.current_state != CSVState::RECORD_SEPARATOR && carriage_return;
	column_count -= (column_count - 1) * (states.current_state != CSVState::RECORD_SEPARATOR && carriage_return);

	// Identify what is our line separator
	state_machine->carry_on_separator =
	    (states.current_state == CSVState::RECORD_SEPARATOR && carriage_return) || state_machine->carry_on_separator;
	state_machine->single_record_separator =
	    ((states.current_state != CSVState::RECORD_SEPARATOR && carriage_return) ||
	     (states.current_state == CSVState::RECORD_SEPARATOR && !carriage_return)) ||
	    state_machine->single_record_separator;
	if (result.cur_rows >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
	return false;
}

void DialectScanner::Process() {
	// Run on this buffer
	for (; pos.pos < cur_buffer_handle->actual_size; pos.pos++) {
		if (ProcessInternal(buffer_handle_ptr[pos.pos])) {
			return;
		}
	}
}

void DialectScanner::FinalizeChunkProcess() {
	if (result.cur_rows == STANDARD_VECTOR_SIZE) {
		// We are done
		return;
	}
	// We run until we have a full chunk, or we are done scanning
	while (!Finished() && result.cur_rows < STANDARD_VECTOR_SIZE) {
		if (pos.pos == cur_buffer_handle->actual_size) {
			// Move to next buffer
			pos.pos = 0;
			cur_buffer_handle = buffer_manager->GetBuffer(pos.file_id, ++pos.buffer_id);
			buffer_handle_ptr = cur_buffer_handle->Ptr();
		}
		Process();
	}
}
} // namespace duckdb
