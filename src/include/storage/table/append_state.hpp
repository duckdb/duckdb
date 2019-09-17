//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/append_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {
class TransientSegment;

struct ColumnAppendState {
	//! The current segment of the append
	TransientSegment *current;
	//! The current row
	row_t row_start;
};

struct TableAppendState {
	unique_ptr<std::lock_guard<std::mutex>> append_lock;
	unique_ptr<ColumnAppendState[]> states;
	row_t row_start;
	row_t current_row;
};

}
