//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_line_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
struct LineInfo {
public:
	explicit LineInfo(mutex *main_mutex_p) : main_mutex(main_mutex_p) {};
	bool CanItGetLine(idx_t batch_idx);

	idx_t GetLine(idx_t batch_idx);

	//! Lines read per batch, <batch_index,count>
	unordered_map<idx_t, idx_t> lines_read;
	//! Set of batches that have been initialized but are not yet finished.
	set<idx_t> current_batches;
	mutex *main_mutex = nullptr;
};

} // namespace duckdb
