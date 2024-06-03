#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"
#include "duckdb/execution/operator/csv_scanner/column_count_scanner.hpp"

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

void SkipResult::QuotedNewLine(SkipResult &result) {
	// nop
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
	if (result.state_machine.dialect_options.num_cols == 1) {
		return AddRow(result, buffer_pos);
	}
	return false;
}

SkipScanner::SkipScanner(shared_ptr<CSVBufferManager> buffer_manager, const shared_ptr<CSVStateMachine> &state_machine,
                         shared_ptr<CSVErrorHandler> error_handler, idx_t rows_to_skip)
    : BaseScanner(std::move(buffer_manager), state_machine, std::move(error_handler)),
      result(states, *state_machine, rows_to_skip) {
}

SkipResult &SkipScanner::ParseChunk() {
	ParseChunkInternal(result);
	return result;
}

SkipResult &SkipScanner::GetResult() {
	return result;
}

void SkipScanner::Initialize() {
	states.Initialize();
}

void SkipScanner::FinalizeChunkProcess() {
	// We continue skipping until we skipped enough rows, or we have nothing else to read.
	while (!FinishedFile() && result.row_count < result.rows_to_skip) {
		cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
		if (cur_buffer_handle) {
			iterator.pos.buffer_pos = 0;
			buffer_handle_ptr = cur_buffer_handle->Ptr();
			Process(result);
		}
	}
	// Skip Carriage Return
	if (state_machine->options.dialect_options.state_machine_options.new_line == NewLineIdentifier::CARRY_ON &&
	    states.states[1] == CSVState::CARRIAGE_RETURN) {
		iterator.pos.buffer_pos++;
	}
	iterator.done = FinishedFile();
}
} // namespace duckdb
