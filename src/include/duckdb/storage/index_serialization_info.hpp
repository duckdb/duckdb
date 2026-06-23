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

enum ARTSerializationFormat { V1_0_0, CURRENT };

struct CheckpointedIndex {
	shared_ptr<const IndexStorageInfo> storage_info;
	unique_ptr<BoundIndex>	shadow_index;
};

} // namespace duckdb
