//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/bloom_scan_probe.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"
#include "duckdb/storage/table/index_entry.hpp"

namespace duckdb {

class TableFilter;

struct BloomScanProbe {
	const TableFilter *filter = nullptr;
	unique_ptr<IndexReadHandle<BoundIndex>> handle;
	idx_t column_index = 0;
	hash_t hash = 0;
	bool usable = false;
	bool fallback = false;
};

} // namespace duckdb
