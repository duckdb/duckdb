#include "duckdb/execution/operator/scan/csv/scanner/column_count_scanner.hpp"

namespace duckdb {

ColumnCountResult::ColumnCountResult(CSVStates &states, CSVStateMachine &state_machine)
    : ScannerResult(states, state_machine) {
}

void ColumnCountResult::AddValue(ColumnCountResult &result, const idx_t buffer_pos) {
	result.current_column_count++;
}

inline void ColumnCountResult::InternalAddRow() {
	column_counts[result_position++] = current_column_count + 1;
	current_column_count = 0;
}

idx_t &ColumnCountResult::operator[](size_t index) {
	return column_counts[index];
}

bool ColumnCountResult::AddRow(ColumnCountResult &result, const idx_t buffer_pos) {
	result.InternalAddRow();

	// This is hacky, should be probably moved somewhere.
	result.state_machine.carry_on_separator =
	    result.states.current_state == CSVState::CARRIAGE_RETURN || result.state_machine.carry_on_separator;
	result.state_machine.single_record_separator =
	    result.states.current_state == CSVState::RECORD_SEPARATOR || result.state_machine.single_record_separator;

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
    : BaseScanner(buffer_manager, state_machine), result(states, *state_machine), column_count(1) {

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

void ColumnCountScanner::Initialize() {
	states.Initialize(CSVState::EMPTY_LINE);
}

void ColumnCountScanner::Process() {
	// Run on this buffer
	for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result)) {
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
	while (!FinishedFile() && result.result_position < STANDARD_VECTOR_SIZE) {
		if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size) {
			// Move to next buffer
			iterator.pos.buffer_pos = 0;
			cur_buffer_handle = buffer_manager->GetBuffer(iterator.pos.file_idx, ++iterator.pos.buffer_idx);
			if (!cur_buffer_handle) {
				buffer_handle_ptr = nullptr;
				// This means we reached the end of the file, we must add a last line if there is any to be added
				if (result.current_column_count > 0) {
					result.InternalAddRow();
				}
				return;
			}
			buffer_handle_ptr = cur_buffer_handle->Ptr();
		}
		Process();
	}
}
} // namespace duckdb
