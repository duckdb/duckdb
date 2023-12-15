//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/parsing/scanner_boundary.hpp
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
struct ScannerBoundary {
	ScannerBoundary(idx_t file_idx_p, idx_t buffer_idx_p, idx_t buffer_pos_p, idx_t bytes_to_read_p)
	    : file_idx(file_idx_p), start_buffer_idx(buffer_idx_p), start_buffer_pos(buffer_pos_p),
	      buffer_idx(buffer_idx_p), buffer_pos(buffer_pos_p){};
	//! Constructor used for the first CSV Iterator of a scanner
	ScannerBoundary(idx_t start_buffer_pos_p, idx_t bytes_to_read_p)
	    : start_buffer_pos(start_buffer_pos_p), buffer_pos(start_buffer_pos_p){};
	ScannerBoundary() {};

	//! Resets the Iterator, only used in the sniffing where scanners must be restarted for dialect/type detection
	void Reset();

//	//! Moves the Iterator to the next positions
//	//! There are three options for the iterator movement.
//	//! 1) We are done with the current file, hence we move to the next file
//	//! 2) We are done with the current buffer, hence we move to the next buffer
//	//! 3) We are not done with the current buffer, hence we just move where we start within the buffer
//	bool Next(CSVBufferManager &buffer_manager);

	//! I'm already done man, please stop scanning me
	bool Done();

	//! File index where we start scanning [0-idx], a scanner can never go over one file.
	const idx_t file_idx = 0;
	//! Start Buffer index of the file where we start scanning
	const idx_t start_buffer_idx = 0;
	//! Current Buffer index of the file we are scanning
	const idx_t buffer_idx = 0;
	//! Start Buffer position of the buffer of the file where we start scanning
	const idx_t start_buffer_pos = 0;
	//! Initially set end position
	const idx_t real_end_pos;
	//fixme: Do I actually need this?
	//! The id of this iterator
	const idx_t iterator_id = 0;
	//! Last position this iterator should read.
	const idx_t end_pos;
	//! Current Buffer position of the buffer of the file we are scanning
	idx_t current_position = 0;
	//! This is a naughty naughy boy that needs to go over the buffer it was initially assigned
	bool over_buffer = false;
};
