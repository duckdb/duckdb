#include "duckdb/execution/operator/csv_scanner/base_scanner.hpp"

#include "duckdb/execution/operator/csv_scanner/csv_sniffer.hpp"
#include "duckdb/execution/operator/csv_scanner/skip_scanner.hpp"

namespace duckdb {

ScannerResult::ScannerResult(CSVStates &states_p, CSVStateMachine &state_machine_p)
    : state_machine(state_machine_p), states(states_p) {
}

BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         shared_ptr<CSVErrorHandler> error_handler_p, bool sniffing_p,
                         shared_ptr<CSVFileScan> csv_file_scan_p, CSVIterator iterator_p)
    : csv_file_scan(std::move(csv_file_scan_p)), sniffing(sniffing_p), error_handler(std::move(error_handler_p)),
      state_machine(std::move(state_machine_p)), buffer_manager(std::move(buffer_manager_p)), iterator(iterator_p) {
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

CSVIterator BaseScanner::SkipCSVRows(shared_ptr<CSVBufferManager> buffer_manager,
                                     const shared_ptr<CSVStateMachine> &state_machine, idx_t rows_to_skip) {
	if (rows_to_skip == 0) {
		return {};
	}
	auto error_handler = make_shared_ptr<CSVErrorHandler>();
	SkipScanner row_skipper(std::move(buffer_manager), state_machine, error_handler, rows_to_skip);
	row_skipper.ParseChunk();
	return row_skipper.GetIterator();
}

CSVIterator &BaseScanner::GetIterator() {
	return iterator;
}

void BaseScanner::SetIterator(const CSVIterator &it) {
	iterator = it;
}

ScannerResult &BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is not implemented");
}

ScannerResult &BaseScanner::GetResult() {
	throw InternalException("GetResult() from CSV Base Scanner is not implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is not implemented");
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is not implemented");
}

CSVStateMachine &BaseScanner::GetStateMachine() {
	return *state_machine;
}

} // namespace duckdb
