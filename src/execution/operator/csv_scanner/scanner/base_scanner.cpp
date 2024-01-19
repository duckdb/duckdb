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

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

CSVStateMachine &BaseScanner::GetStateMachine() {
	return *state_machine;
}

} // namespace duckdb
