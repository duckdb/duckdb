//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/hive_partitioning.hpp"

namespace duckdb {
struct BindInfo;
class MultiFileList;

enum class MultiFileColumnMappingMode : uint8_t { BY_NAME, BY_FIELD_ID };

struct MultiFileOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool auto_detect_hive_partitioning = true;
	//! Whether directories whose hive partition keys cannot match a filter are skipped without being listed
	bool hive_directory_pruning = false;
	//! Whether hive_directory_pruning was set explicitly - if it was not, the setting of the same name decides
	bool explicit_hive_directory_pruning = false;
	bool union_by_name = false;
	bool hive_types_autocast = true;
	bool allow_empty = false;
	MultiFileColumnMappingMode mapping = MultiFileColumnMappingMode::BY_NAME;

	case_insensitive_map_t<LogicalType> hive_types_schema;

	// Default/configurable name of the column containing the file names
	static constexpr const char *DEFAULT_FILENAME_COLUMN = "filename";
	string filename_column = DEFAULT_FILENAME_COLUMN;
	// These are used to pass options through custom multifilereaders
	case_insensitive_map_t<Value> custom_options;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileOptions Deserialize(Deserializer &source);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info) const;
	DUCKDB_API void AutoDetectHivePartitioning(MultiFileList &files, ClientContext &context);
	DUCKDB_API static bool AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context,
	                                                          optional_idx sample_size = optional_idx());
	DUCKDB_API void AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context);
	//! Take over the value of the "hive_directory_pruning" setting unless the option was given explicitly - must be
	//! called once the options have been parsed, directories are never pruned without it
	DUCKDB_API void ResolveHiveDirectoryPruning(ClientContext &context);
	//! The number of files that is inspected while binding - only a sample when directories can still be pruned
	//! afterwards, since expanding the entire file list there would defeat that pruning
	DUCKDB_API optional_idx GetBindSampleSize() const;
	DUCKDB_API void VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const;
	DUCKDB_API LogicalType GetHiveLogicalType(const string &hive_partition_column) const;
	DUCKDB_API Value GetHivePartitionValue(const string &base, const string &entry, ClientContext &context) const;
	DUCKDB_API bool AnySet() const;
};

} // namespace duckdb
