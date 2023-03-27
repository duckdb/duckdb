//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
class FieldWriter;
class FieldReader;
struct BindInfo;

struct MultiFileReaderOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool union_by_name = false;

	DUCKDB_API void Serialize(FieldWriter &writer) const;
	DUCKDB_API void Deserialize(FieldReader &reader);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info);
};

} // namespace duckdb
