#include "duckdb/execution/operator/csv_scanner/column_count_scanner.hpp"

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

bool ColumnCountResult::AddRow(ColumnCountResult &result, const idx_t buffer_pos) {
	result.InternalAddRow();
	if (!result.states.EmptyLastValue()) {
		result.last_value_always_empty = false;
	}
	if (result.result_position >= STANDARD_VECTOR_SIZE) {
		// We sniffed enough rows
		return true;
	}
	return false;
}

void ColumnCountResult::InvalidState(ColumnCountResult &result) {
	result.result_position = 0;
	result.error = true;
}

bool ColumnCountResult::EmptyLine(ColumnCountResult &result, const idx_t buffer_pos) {
	// nop
	return false;
}

void ColumnCountResult::QuotedNewLine(ColumnCountResult &result) {
	// nop
}

ColumnCountScanner::ColumnCountScanner(shared_ptr<CSVBufferManager> buffer_manager_p,
                                       const shared_ptr<CSVStateMachine> &state_machine,
                                       shared_ptr<CSVErrorHandler> error_handler, CSVIterator iterator)
    : BaseScanner(std::move(buffer_manager_p), state_machine, std::move(error_handler), true, nullptr, iterator),
      result(states, *state_machine), column_count(1) {
}

unique_ptr<StringValueScanner> ColumnCountScanner::UpgradeToStringValueScanner() {
	auto iterator = SkipCSVRows(buffer_manager, state_machine, state_machine->dialect_options.skip_rows.GetValue());
	if (iterator.done) {
		return make_uniq<StringValueScanner>(0U, buffer_manager, state_machine, error_handler, nullptr, true);
	}
	return make_uniq<StringValueScanner>(0U, buffer_manager, state_machine, error_handler, nullptr, true, iterator);
}

ColumnCountResult &ColumnCountScanner::ParseChunk() {
	result.result_position = 0;
	column_count = 1;
	ParseChunkInternal(result);
	return result;
}

ColumnCountResult &ColumnCountScanner::GetResult() {
	return result;
}

void ColumnCountScanner::Initialize() {
	states.Initialize();
}

void ColumnCountScanner::FinalizeChunkProcess() {
	if (result.result_position == STANDARD_VECTOR_SIZE || result.error) {
		// We are done
		return;
	}
	// We run until we have a full chunk, or we are done scanning
	while (!FinishedFile() && result.result_position < STANDARD_VECTOR_SIZE && !result.error) {
		if (iterator.pos.buffer_pos == cur_buffer_handle->actual_size) {
			// Move to next buffer
			cur_buffer_handle = buffer_manager->GetBuffer(++iterator.pos.buffer_idx);
			if (!cur_buffer_handle) {
				buffer_handle_ptr = nullptr;
				if (states.EmptyLine() || states.NewRow() || states.IsCurrentNewRow() || states.IsNotSet()) {
					return;
				}
				// This means we reached the end of the file, we must add a last line if there is any to be added
				result.InternalAddRow();
				return;
			}
			iterator.pos.buffer_pos = 0;
			buffer_handle_ptr = cur_buffer_handle->Ptr();
		}
		Process(result);
	}
}
} // namespace duckdb
