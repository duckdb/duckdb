//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/append_state.hpp"

namespace duckdb {
class TableCatalogEntry;

struct TableUpdateState {
	unique_ptr<ConstraintState> constraint_state;
};

} // namespace duckdb
