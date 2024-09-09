//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_validator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"

namespace duckdb {

//! Information used to validate
struct ValidatorLine {
	ValidatorLine(idx_t start_pos_p, idx_t end_pos_p) : start_pos(start_pos_p), end_pos(end_pos_p) {
	}
	const idx_t start_pos;
	const idx_t end_pos;
};

struct ThreadLines {
public:
	ThreadLines() {};
	//! Validate everything is as it should be, returns true if it's all good, false o.w.
	bool Validate();

	void Insert(idx_t thread, ValidatorLine line_info);

private:
	map<idx_t, ValidatorLine> thread_lines;
	//! We allow up to 2 bytes of error margin (basically \r\n)
	static constexpr idx_t error_margin = 2;
};

//! The validator works by double-checking that threads started and ended in the right positions
struct CSVValidator {
	CSVValidator() {
	}
	//! Validate that all files are good
	bool Validate();

	//! Inserts line_info to a given thread index of a given file.
	void Insert(idx_t file_idx, idx_t thread, ValidatorLine line_info);

private:
	//! Per file thread lines.
	vector<ThreadLines> per_file_thread_lines;
};

} // namespace duckdb
