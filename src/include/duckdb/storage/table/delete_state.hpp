//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/delete_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableDeleteState {
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	bool has_delete_constraints = false;
	DataChunk verify_chunk;
	vector<column_t> col_ids;
};

} // namespace duckdb
