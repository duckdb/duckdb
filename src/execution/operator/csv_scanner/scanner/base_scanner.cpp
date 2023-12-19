#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {
BaseScanner::BaseScanner(shared_ptr<CSVBufferManager> buffer_manager_p, shared_ptr<CSVStateMachine> state_machine_p,
                         ScannerBoundary boundary_p)
    : boundary(boundary_p), buffer_manager(buffer_manager_p), state_machine(state_machine_p) {
	D_ASSERT(buffer_manager);
	D_ASSERT(state_machine);
};

bool BaseScanner::Finished() {
	if (!boundary.scan_to_infinity) {
		// If we don't have to scan the whole file we are done if we either went to next buffer or if our current
		// position is over the end position of our boundary.
		return pos.buffer_id > boundary.buffer_idx || pos.pos >= boundary.end_pos;
	}
	if (buffer_manager->FileCount() > 1) {
		//! Fixme: We might want to lift this if we want to run the sniffer over multiple files.
		throw InternalException("We can't have a buffer manager that scans to infinity with more than one file");
	}
	// we have to scan to infinity, so we must check if we are done checking the whole file
	if (!buffer_manager->Done()) {
		return false;
	}
	// If yes, are we in the last buffer?
	if (pos.buffer_id + 1 != buffer_manager->CachedBufferPerFile(pos.file_id)) {
		return false;
	}
	// If yes, are we in the last position?
	return pos.pos + 1 == cur_buffer_handle->actual_size;
}

void BaseScanner::Reset() {
	pos.buffer_id = boundary.buffer_idx;
	pos.pos = boundary.buffer_pos;
}

ScannerResult *BaseScanner::ParseChunk() {
	throw InternalException("ParseChunk() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Initialize() {
	throw InternalException("Initialize() from CSV Base Scanner is mot implemented");
}

void BaseScanner::Process() {
	throw InternalException("Process() from CSV Base Scanner is mot implemented");
}

void BaseScanner::FinalizeChunkProcess() {
	throw InternalException("FinalizeChunkProcess() from CSV Base Scanner is mot implemented");
}

void BaseScanner::ParseChunkInternal() {
	if (!initialized) {
		Initialize();
	}
	Process();
	FinalizeChunkProcess();
}

} // namespace duckdb
