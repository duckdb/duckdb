//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
struct BindInfo;

struct MultiFileReaderOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool union_by_name = false;
	bool hive_types = false;
	bool hive_types_auto_detect = false;
	case_insensitive_map_t<LogicalType> hive_types_schema;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderOptions Deserialize(Deserializer &source);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info) const;

};

} // namespace duckdb
