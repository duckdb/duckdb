//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/index_serialization_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/index_storage_info.hpp"

namespace duckdb {

enum IndexSerializationFormat { V1_0_0, CURRENT };

// When serializing indexes, new IndexStorageInfos are created upon BoundIndex serialization, whereas for
// UnboundIndex, IndexStorageInfo already exists inside the UnboundIndex.
// We want to serialize IndexStorageInfo's in the same order that we serialized indexes, which is stored as
// a vector of references in the ordered_infos field here.
// UnboundIndexes still "own" the IndexStorageInfo and so a reference can just be directly pushed.
// For BoundIndexes, however, we need to keep the newly created IndexStorageInfo's alive, and so they
// are stored in this result type. When a BoundIndex is added to bound_infos, a reference to this is then
// pushed to ordered_infos.
struct IndexSerializationResult {
	//! The ordered list of references to serialize - preserves iteration order of index_entries
	vector<reference<const IndexStorageInfo>> ordered_infos;
	//! Storage for bound index infos to keep them alive.
	vector<IndexStorageInfo> bound_infos;
};

} // namespace duckdb
