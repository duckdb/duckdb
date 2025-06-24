//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/delete_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class DataTable;
class RowVersionManager;

struct DeleteInfo {
	DataTable *table;
	RowVersionManager *version_info;
	idx_t vector_idx;
	idx_t count;
	idx_t base_row;
	//! Whether or not row ids are consecutive (0, 1, 2, ..., count).
	//! If this is true no rows are stored and `rows` should not be accessed.
	bool is_consecutive;

	uint16_t *GetRows() {
		if (is_consecutive) {
			throw InternalException("DeleteInfo is consecutive - rows are not accessible");
		}
		return rows;
	}
	const uint16_t *GetRows() const {
		if (is_consecutive) {
			throw InternalException("DeleteInfo is consecutive - rows are not accessible");
		}
		return rows;
	}

private:
	//! The per-vector row identifiers (actual row id is base_row + rows[x])
	uint16_t rows[1];
};

} // namespace duckdb
