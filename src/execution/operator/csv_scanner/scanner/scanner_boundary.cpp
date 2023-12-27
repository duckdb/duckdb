#include "duckdb/execution/operator/scan/csv/scanner/scanner_boundary.hpp"

namespace duckdb {

ScannerBoundary::ScannerBoundary(idx_t file_idx_p, idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t boundary_idx_p)
    : file_idx(file_idx_p), buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p), is_set(true),
      boundary_idx(boundary_idx_p) {
	end_pos = buffer_pos + BYTES_PER_THREAD;
};

ScannerBoundary::ScannerBoundary() : file_idx(0), buffer_idx(0), buffer_pos(0), is_set(false) {};

bool ScannerBoundary::Next(CSVBufferManager &buffer_manager) {
	if (file_idx >= buffer_manager.FileCount()) {
		// We are done
		return false;
	}
	boundary_idx++;
	// This is our start buffer
	auto buffer = buffer_manager.GetBuffer(file_idx, buffer_idx);
	if (buffer->is_last_buffer && buffer_pos + ScannerBoundary::BYTES_PER_THREAD > buffer->actual_size) {
		// 1) We are done with the current file, we must move to the next file
		// We are done with this file, we need to reset everything for the next file
		file_idx++;
		if (file_idx >= buffer_manager.FileCount()) {
			// We are done
			return false;
		}
		buffer_idx = 0;
		buffer_pos = buffer_manager.GetStartPos();
	} else if (buffer_pos + BYTES_PER_THREAD > buffer->actual_size) {
		// 2) We still have data to scan in this file, we set the iterator accordingly.
		// We must move the buffer
		buffer_idx++;
		buffer_pos = buffer_manager.GetStartPos();

	} else {
		// 3) We are not done with the current buffer, hence we just move where we start within the buffer
		buffer_pos += BYTES_PER_THREAD;
	}
	return true;
}

bool ScannerBoundary::IsSet() const {
	return is_set;
}
idx_t ScannerBoundary::GetEndPos() const {
	return end_pos;
}

idx_t ScannerBoundary::GetFileIdx() const {
	return file_idx;
}
idx_t ScannerBoundary::GetBufferPos() const {
	return buffer_pos;
}
idx_t ScannerBoundary::GetBufferIdx() const {
	return buffer_idx;
}

idx_t ScannerBoundary::GetBoundaryIdx() const {
	return boundary_idx;
}

void ScannerBoundary::SetEndPos(idx_t end_pos_p) {
	end_pos = end_pos_p;
}

bool ScannerBoundary::InBoundary(idx_t file_idx_p, idx_t buffer_id_p, idx_t buffer_pos_p) const {
	return file_idx_p == file_idx && buffer_id_p == buffer_idx && buffer_pos_p == buffer_pos;
}

} // namespace duckdb
