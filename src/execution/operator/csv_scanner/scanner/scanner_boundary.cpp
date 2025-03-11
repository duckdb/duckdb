#include "duckdb/execution/operator/csv_scanner/scanner_boundary.hpp"

namespace duckdb {

CSVPosition::CSVPosition(idx_t buffer_idx_p, idx_t buffer_pos_p) : buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p) {
}
CSVPosition::CSVPosition() {
}

CSVBoundary::CSVBoundary(idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t boundary_idx_p, idx_t end_pos_p)
    : buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p), boundary_idx(boundary_idx_p), end_pos(end_pos_p) {
}
CSVBoundary::CSVBoundary() : buffer_idx(0), buffer_pos(0), boundary_idx(0), end_pos(NumericLimits<idx_t>::Maximum()) {
}

CSVIterator::CSVIterator() : buffer_size(0), is_set(false) {
}

void CSVBoundary::Print() const {
#ifndef DUCKDB_DISABLE_PRINT
	std::cout << "---Boundary: " << boundary_idx << " ---" << '\n';
	std::cout << "Buffer Index: " << buffer_idx << '\n';
	std::cout << "Buffer Pos: " << buffer_pos << '\n';
	std::cout << "End Pos: " << end_pos << '\n';
	std::cout << "------------" << end_pos << '\n';
#endif
}

void CSVIterator::Print() const {
#ifndef DUCKDB_DISABLE_PRINT
	boundary.Print();
	std::cout << "Is set: " << is_set << '\n';
#endif
}

idx_t CSVIterator::BytesPerThread(const CSVReaderOptions &reader_options) {
	const idx_t buffer_size = reader_options.buffer_size_option.GetValue();
	const idx_t max_row_size = reader_options.maximum_line_size.GetValue();
	const idx_t bytes_per_thread = buffer_size / CSVBuffer::ROWS_PER_BUFFER * ROWS_PER_THREAD;
	if (bytes_per_thread < max_row_size) {
		// If we are setting up the buffer size directly, we must make sure each thread will read the full buffer.
		return max_row_size;
	}
	return bytes_per_thread;
}

bool CSVIterator::Next(CSVBufferManager &buffer_manager, const CSVReaderOptions &reader_options) {
	if (!is_set) {
		return false;
	}
	const auto bytes_per_thread = BytesPerThread(reader_options);

	// If we are calling next this is not the first one anymore
	first_one = false;
	boundary.boundary_idx++;
	// This is our start buffer
	auto buffer = buffer_manager.GetBuffer(boundary.buffer_idx);
	if (buffer->is_last_buffer && boundary.buffer_pos + bytes_per_thread > buffer->actual_size) {
		// 1) We are done with the current file
		return false;
	} else if (boundary.buffer_pos + bytes_per_thread >= buffer->actual_size) {
		// 2) We still have data to scan in this file, we set the iterator accordingly.
		// We must move the buffer
		boundary.buffer_idx++;
		boundary.buffer_pos = 0;
		// Verify this buffer really exists
		auto next_buffer = buffer_manager.GetBuffer(boundary.buffer_idx);
		if (!next_buffer) {
			return false;
		}

	} else {
		// 3) We are not done with the current buffer, hence we just move where we start within the buffer
		boundary.buffer_pos += bytes_per_thread;
	}
	boundary.end_pos = boundary.buffer_pos + bytes_per_thread;
	SetCurrentPositionToBoundary();
	return true;
}

bool CSVIterator::IsBoundarySet() const {
	return is_set;
}
idx_t CSVIterator::GetEndPos() const {
	return boundary.end_pos;
}

idx_t CSVIterator::GetBufferIdx() const {
	return pos.buffer_idx;
}

idx_t CSVIterator::GetBoundaryIdx() const {
	return boundary.boundary_idx;
}

void CSVIterator::SetCurrentPositionToBoundary() {
	pos.buffer_idx = boundary.buffer_idx;
	pos.buffer_pos = boundary.buffer_pos;
}

void CSVIterator::SetCurrentBoundaryToPosition(bool single_threaded, const CSVReaderOptions &reader_options) {
	if (single_threaded) {
		is_set = false;
		return;
	}
	const auto bytes_per_thread = BytesPerThread(reader_options);

	boundary.buffer_idx = pos.buffer_idx;
	if (pos.buffer_pos == 0) {
		boundary.end_pos = bytes_per_thread;
	} else {
		boundary.end_pos = ((pos.buffer_pos + bytes_per_thread - 1) / bytes_per_thread) * bytes_per_thread;
	}

	boundary.buffer_pos = boundary.end_pos - bytes_per_thread;
	is_set = true;
}

void CSVIterator::SetStart(idx_t start) {
	boundary.buffer_pos = start;
}

void CSVIterator::SetEnd(idx_t pos) {
	boundary.end_pos = pos;
}

void CSVIterator::CheckIfDone() {
	if (IsBoundarySet() && (pos.buffer_idx > boundary.buffer_idx || pos.buffer_pos > boundary.buffer_pos)) {
		done = true;
	}
}

idx_t CSVIterator::GetGlobalCurrentPos() const {
	return pos.buffer_pos + buffer_size * pos.buffer_idx;
}

} // namespace duckdb
