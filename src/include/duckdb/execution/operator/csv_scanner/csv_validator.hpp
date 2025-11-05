//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_validator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Information used to validate
struct ValidatorLine {
	ValidatorLine(idx_t start_pos_p, idx_t end_pos_p) : start_pos(start_pos_p), end_pos(end_pos_p) {
	}
	const idx_t start_pos;
	const idx_t end_pos;
};

struct ThreadLines {
	ThreadLines() {};
	//! Validate everything is as it should be, returns true if it's all good, false o.w.
	void Verify() const;

	void Insert(idx_t thread, ValidatorLine line_info);

	string Print() const;

private:
	map<idx_t, ValidatorLine> thread_lines;
	//! We allow up to 2 bytes of error margin (basically \r\n)
	static constexpr idx_t error_margin = 2;
};

//! The validator works by double-checking that threads started and ended in the right positions
struct CSVValidator {
	CSVValidator() {
	}
	//! Validate that all lines are good
	void Verify() const;

	//! Inserts line_info to a given thread index
	void Insert(idx_t thread, ValidatorLine line_info);

	string Print() const;

private:
	//! Thread lines for this file
	ThreadLines thread_lines;
};

} // namespace duckdb
