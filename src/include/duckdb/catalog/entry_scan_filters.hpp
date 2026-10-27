//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/entry_scan_filters.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class CatalogEntry;
class ClientContext;
class TableFilter;

//! Filters pushed into a system catalog scan (duckdb_tables, duckdb_views, duckdb_columns) on the entry identity
//! columns: the entry's name (table_name / view_name) and oid (table_oid / view_oid).
struct EntryScanFilters {
	optional_ptr<const TableFilter> name;
	optional_ptr<const TableFilter> oid;

	//! Whether the entry satisfies every filter present; a missing filter accepts everything.
	bool Accepts(ClientContext &context, const CatalogEntry &entry) const;
};

} // namespace duckdb
