#include "duckdb/execution/operator/scan/csv/scanner/column_count_scanner.hpp"

namespace duckdb {

void ColumnCountResult::AddValue(ColumnCountResult &result, const char current_char, const idx_t buffer_pos) {
	result.column_counts[result.cur_rows]++;
}

bool ColumnCountResult::AddRow(ColumnCountResult &result, const char current_char, const idx_t buffer_pos) {
	result.column_counts[result.cur_rows++]++;

	// This is hacky, should be probably moved somewhere.
	result.state_machine->carry_on_separator = current_char == '\r' || result.state_machine->carry_on_separator;
	result.state_machine->single_record_separator =
	    current_char == '\n' || result.state_machine->single_record_separator;

	if (result.cur_rows >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
}

void ColumnCountResult::Kaput(ColumnCountResult &result) {
	result.cur_rows = 0;
}

ColumnCountScanner::ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine), column_count(1) {
	result.cur_rows = 0;
	result.state_machine = state_machine.get();
};

ColumnCountResult *ColumnCountScanner::ParseChunk() {
	result.cur_rows = 0;
	column_count = 1;
	ParseChunkInternal();
	return &result;
}

void ColumnCountScanner::Process() {
	// Run on this buffer
	for (; pos.pos < cur_buffer_handle->actual_size; pos.pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[pos.pos], pos.pos, result)) {
			return;
		}
	}
}

void ColumnCountScanner::FinalizeChunkProcess() {
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
