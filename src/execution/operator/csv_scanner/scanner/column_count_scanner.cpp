#include "duckdb/execution/operator/scan/csv/scanner/column_count_scanner.hpp"

namespace duckdb {

void ColumnCountResult::AddValue(ColumnCountResult &result, const char current_char, const idx_t buffer_pos) {
	result.column_counts[result.result_position]++;
}

idx_t &ColumnCountResult::operator[](size_t index) {
	return column_counts[index];
}

bool ColumnCountResult::AddRow(ColumnCountResult &result, const char current_char, const idx_t buffer_pos) {
	result.column_counts[result.result_position++]++;

	// This is hacky, should be probably moved somewhere.
	result.state_machine->carry_on_separator = current_char == '\r' || result.state_machine->carry_on_separator;
	result.state_machine->single_record_separator =
	    current_char == '\n' || result.state_machine->single_record_separator;

	if (result.result_position >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
	return false;
}

void ColumnCountResult::Kaput(ColumnCountResult &result) {
	result.result_position = 0;
}

ColumnCountScanner::ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager,
                                       shared_ptr<CSVStateMachine> state_machine)
    : BaseScanner(buffer_manager, state_machine), column_count(1) {
	result.result_position = 0;
	result.state_machine = state_machine.get();
};

unique_ptr<StringValueScanner> ColumnCountScanner::UpgradeToStringValueScanner() {
	return make_uniq<StringValueScanner>(buffer_manager, state_machine);
}

ColumnCountResult *ColumnCountScanner::ParseChunk() {
	result.result_position = 0;
	column_count = 1;
	ParseChunkInternal();
	return &result;
}

ColumnCountResult *ColumnCountScanner::GetResult() {
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
	if (result.result_position == STANDARD_VECTOR_SIZE) {
		// We are done
		return;
	}
	// We run until we have a full chunk, or we are done scanning
	while (!Finished() && result.result_position < STANDARD_VECTOR_SIZE) {
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
