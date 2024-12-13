//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
struct BindInfo;
class MultiFileList;

enum class MultiFileReaderColumnMappingMode : uint8_t { BY_NAME, BY_FIELD_ID };

struct MultiFileReaderOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool auto_detect_hive_partitioning = true;
	bool union_by_name = false;
	bool hive_types_autocast = true;
	MultiFileReaderColumnMappingMode mapping = MultiFileReaderColumnMappingMode::BY_NAME;

	case_insensitive_map_t<LogicalType> hive_types_schema;

	// Default/configurable name of the column containing the file names
	static constexpr const char *DEFAULT_FILENAME_COLUMN = "filename";
	string filename_column = DEFAULT_FILENAME_COLUMN;
	// These are used to pass options through custom multifilereaders
	case_insensitive_map_t<Value> custom_options;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderOptions Deserialize(Deserializer &source);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info) const;
	DUCKDB_API void AutoDetectHivePartitioning(MultiFileList &files, ClientContext &context);
	DUCKDB_API static bool AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context);
	DUCKDB_API void AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context);
	DUCKDB_API void VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const;
	DUCKDB_API LogicalType GetHiveLogicalType(const string &hive_partition_column) const;
	DUCKDB_API Value GetHivePartitionValue(const string &base, const string &entry, ClientContext &context) const;
	DUCKDB_API bool AnySet();
};

} // namespace duckdb
