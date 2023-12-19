//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parser/scanner_boundary.hpp
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
	ScannerBoundary(idx_t file_idx_p, idx_t buffer_idx_p, idx_t buffer_pos_p)
	    : file_idx(file_idx_p), buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p), is_set(true) {
		end_pos = buffer_pos + BYTES_PER_THREAD;
	};

	ScannerBoundary() : file_idx(0), buffer_idx(0), buffer_pos(0), is_set(false) {};

	//! 8 MB TODO: Should benchmarks other values
	static constexpr idx_t BYTES_PER_THREAD = 8000000;

	//! File index where we start scanning [0-idx], a scanner can never go over one file.
	const idx_t file_idx = 0;
	//! Start Buffer index of the file where we start scanning
	const idx_t buffer_idx = 0;
	//! Start Buffer position of the buffer of the file where we start scanning
	const idx_t buffer_pos = 0;
	//! Last position this iterator should read.
	idx_t end_pos;
	//! Sometimes life knows no boundaries.
	//! The boundaries don't have to be set for single-threaded execution.
	bool is_set;
};
} // namespace duckdb
