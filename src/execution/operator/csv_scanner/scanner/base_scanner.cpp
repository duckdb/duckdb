#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/scanner/base_scanner.hpp"

namespace duckdb {

ScannerResult::ScannerResult(CSVStates &states_p, CSVStateMachine &state_machine_p)
    : states(states_p), state_machine(state_machine_p) {
}

idx_t ScannerResult::Size() {
	return result_position;
}

bool ScannerResult::Empty() {
	return result_position == 0;
}

BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         shared_ptr<CSVErrorHandler> error_handler_p, CSVIterator iterator_p)
    : error_handler(std::move(error_handler_p)), state_machine(std::move(state_machine_p)), iterator(iterator_p),
      buffer_manager(std::move(buffer_manager_p)) {
	D_ASSERT(buffer_manager);
	D_ASSERT(state_machine);
	// Initialize current buffer handle
	cur_buffer_handle = buffer_manager->GetBuffer(iterator.GetBufferIdx());
	if (!cur_buffer_handle) {
		buffer_handle_ptr = nullptr;
	} else {
		buffer_handle_ptr = cur_buffer_handle->Ptr();
	}
}

bool BaseScanner::FinishedFile() {
	if (!cur_buffer_handle) {
		return true;
	}
	// we have to scan to infinity, so we must check if we are done checking the whole file
	if (!buffer_manager->Done()) {
		return false;
	}
	// If yes, are we in the last buffer?
	if (iterator.pos.buffer_idx != buffer_manager->BufferCount()) {
		return false;
	}
	// If yes, are we in the last position?
	return iterator.pos.buffer_pos + 1 == cur_buffer_handle->actual_size;
}

void BaseScanner::Reset() {
	iterator.SetCurrentPositionToBoundary();
	lines_read = 0;
}

CSVIterator &BaseScanner::GetIterator() {
	return iterator;
}

ScannerResult &BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is mot implemented");
}

ScannerResult &BaseScanner::GetResult() {
	throw InternalException("GetResult() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is mot implemented");
}

template <class T>
void BaseScanner::Process(T &result) {
	idx_t to_pos;
	if (iterator.IsBoundarySet()) {
		to_pos = iterator.GetEndPos();
		if (to_pos > cur_buffer_handle->actual_size) {
			to_pos = cur_buffer_handle->actual_size;
		}
	} else {
		to_pos = cur_buffer_handle->actual_size;
	}
	while (iterator.pos.buffer_pos < to_pos) {
		state_machine->Transition(states, buffer_handle_ptr[iterator.pos.buffer_pos]);
		switch (states.states[1]) {
		case CSVState::INVALID:
			T::InvalidState(result);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::RECORD_SEPARATOR:
			if (states.states[0] == CSVState::RECORD_SEPARATOR) {
				lines_read++;
				if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
					iterator.pos.buffer_pos++;
					return;
				}
			} else if (states.states[0] != CSVState::CARRIAGE_RETURN) {
				lines_read++;
				if (T::AddRow(result, iterator.pos.buffer_pos)) {
					iterator.pos.buffer_pos++;
					return;
				}
			}
			iterator.pos.buffer_pos++;
			break;
		case CSVState::CARRIAGE_RETURN:
			lines_read++;
			if (states.states[0] != CSVState::RECORD_SEPARATOR) {
				if (T::AddRow(result, iterator.pos.buffer_pos)) {
					iterator.pos.buffer_pos++;
					return;
				}
			} else {
				if (T::EmptyLine(result, iterator.pos.buffer_pos)) {
					iterator.pos.buffer_pos++;
					return;
				}
			}
			iterator.pos.buffer_pos++;
			break;
		case CSVState::DELIMITER:
			T::AddValue(result, iterator.pos.buffer_pos);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::QUOTED:
			if (states.states[0] == CSVState::UNQUOTED) {
				T::SetEscaped(result);
			}
			T::SetQuoted(result);
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_quoted[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		case CSVState::ESCAPE:
			T::SetEscaped(result);
			iterator.pos.buffer_pos++;
			break;
		case CSVState::STANDARD:
			iterator.pos.buffer_pos++;
			while (state_machine->transition_array
			           .skip_standard[static_cast<uint8_t>(buffer_handle_ptr[iterator.pos.buffer_pos])] &&
			       iterator.pos.buffer_pos < to_pos - 1) {
				iterator.pos.buffer_pos++;
			}
			break;
		default:
			iterator.pos.buffer_pos++;
			break;
		}
	}
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

template <class T>
void BaseScanner::ParseChunkInternal(T &result) {
	if (!initialized) {
		Initialize();
		initialized = true;
	}
	Process(result);
	FinalizeChunkProcess();
}

CSVStateMachine &BaseScanner::GetStateMachine() {
	return *state_machine;
}

} // namespace duckdb
