#include "duckdb/execution/operator/csv_scanner/scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/column_count_scanner.hpp"

namespace duckdb {

SkipResult::SkipResult(CSVStates &states, CSVStateMachine &state_machine, idx_t rows_to_skip_p)
    : ScannerResult(states, state_machine), rows_to_skip(rows_to_skip_p) {
}

void SkipResult::AddValue(SkipResult &result, const idx_t buffer_pos) {
	// nop
}

inline void SkipResult::InternalAddRow() {
	row_count++;
}

bool SkipResult::AddRow(SkipResult &result, const idx_t buffer_pos) {
	result.InternalAddRow();
	if (result.row_count >= result.rows_to_skip) {
		// We skipped enough rows
		return true;
	}
	return false;
}

void SkipResult::InvalidState(SkipResult &result) {
	// nop
}

bool SkipResult::EmptyLine(SkipResult &result, const idx_t buffer_pos) {
	return AddRow(result, buffer_pos);
}
SkipScanner::SkipScanner(shared_ptr<CSVBufferManager> buffer_manager, shared_ptr<CSVStateMachine> state_machine,
                         shared_ptr<CSVErrorHandler> error_handler, idx_t rows_to_skip)
    : BaseScanner(buffer_manager, state_machine, error_handler), result(states, *state_machine, rows_to_skip) {

                                                                 };

SkipResult *SkipScanner::ParseChunk() {
	ParseChunkInternal();
	return &result;
}

SkipResult *SkipScanner::GetResult() {
	return &result;
}

void SkipScanner::Initialize() {
	states.Initialize(CSVState::RECORD_SEPARATOR);
}

void SkipScanner::Process() {
	// Run on this buffer
	for (; iterator.pos.buffer_pos < cur_buffer_handle->actual_size; iterator.pos.buffer_pos++) {
		if (ProcessCharacter(*this, buffer_handle_ptr[iterator.pos.buffer_pos], iterator.pos.buffer_pos, result)) {
			return;
		}
	}
}

void SkipScanner::FinalizeChunkProcess() {
	if (result.rows_to_skip == result.row_count) {
		// We are done
		return;
	}
}
} // namespace duckdb
