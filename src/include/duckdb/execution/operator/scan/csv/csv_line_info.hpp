//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_line_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
struct LineInfo {
public:
	explicit LineInfo(mutex &main_mutex_p, vector<unordered_map<idx_t, idx_t>> &batch_to_tuple_end_p,
	                  vector<set<idx_t>> &tuple_start_p, vector<vector<idx_t>> &tuple_end_p)
	    : main_mutex(main_mutex_p), batch_to_tuple_end(batch_to_tuple_end_p), tuple_start(tuple_start_p),
	      tuple_end(tuple_end_p) {};
	bool CanItGetLine(idx_t file_idx, idx_t batch_idx);

	//! Return the 1-indexed line number
	idx_t GetLine(idx_t batch_idx, idx_t line_error = 0, idx_t file_idx = 0, idx_t cur_start = 0, bool verify = true,
	              bool stop_at_first = true);
	//! Verify if the CSV File was read correctly from [0,batch_idx] batches.
	void Verify(idx_t file_idx, idx_t batch_idx, idx_t cur_first_pos);
	//! Lines read per batch, <file_index, <batch_index, count>>
	vector<unordered_map<idx_t, idx_t>> lines_read;
	//! Set of batches that have been initialized but are not yet finished.
	vector<set<idx_t>> current_batches;
	//! Pointer to CSV Reader Mutex
	mutex &main_mutex;
	//! Pointer Batch to Tuple End
	vector<unordered_map<idx_t, idx_t>> &batch_to_tuple_end;
	//! Pointer Batch to Tuple Start
	vector<set<idx_t>> &tuple_start;
	//! Pointer Batch to Tuple End
	vector<vector<idx_t>> &tuple_end;
	//! If we already threw an exception on a previous thread.
	bool done = false;
	idx_t first_line = 0;
};

} // namespace duckdb
