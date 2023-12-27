//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/scanner/scanner_boundary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_buffer_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_file_handle.hpp"
#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/fast_mem.hpp"

//! We all need boundaries every now and then, CSV Scans also need them
//! This class keeps track of what a scan should read, so which buffer and from where to where
//! As in real life, much like my childhood country, the rules are not really enforced.
//! So the end boundaries of a Scanner Boundary can and will be pushed.
//! In practice this means that a scanner is tolerated to read one line over it's end.
namespace duckdb {
struct ScannerBoundary {
public:
	ScannerBoundary(idx_t file_idx, idx_t buffer_idx, idx_t buffer_pos, idx_t boundary_idx);

	ScannerBoundary();

	//! Moves this boundary to the next one to be scanned
	bool Next(CSVBufferManager &buffer_manager);

	//! If boundary is set
	bool IsSet() const;

	//! Getters
	idx_t GetEndPos() const;
	idx_t GetFileIdx() const;
	idx_t GetBufferPos() const;
	idx_t GetBufferIdx() const;
	idx_t GetBoundaryIdx() const;

	//! Setters
	void SetEndPos(idx_t end_pos);

	//! If position is within the boundary
	bool InBoundary(idx_t file_idx, idx_t buffer_id, idx_t buffer_pos) const;
	//! 8 MB TODO: Should benchmarks other values
	static constexpr idx_t BYTES_PER_THREAD = 8000000;

private:
	//! File index where we start scanning [0-idx], a scanner can never go over one file.
	idx_t file_idx = 0;
	//! Start Buffer index of the file where we start scanning
	idx_t buffer_idx = 0;
	//! Start Buffer position of the buffer of the file where we start scanning
	idx_t buffer_pos = 0;
	//! The boundary index relative to the total scan, only used for parallel reading to enforce
	//! Insertion Order
	idx_t boundary_idx = 0;
	//! Last position this iterator should read.
	idx_t end_pos;
	//! Sometimes life knows no boundaries.
	//! The boundaries don't have to be set for single-threaded execution.
	bool is_set;
};
} // namespace duckdb
