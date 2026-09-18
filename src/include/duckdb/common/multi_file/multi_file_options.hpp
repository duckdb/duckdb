//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
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
	bool union_by_name = false;
	bool hive_types_autocast = true;
	bool allow_empty = false;
	//! The maximum number of files that are opened to determine the schema - the schemas of the sampled files are
	//! combined into one. NumericLimits<idx_t>::Maximum() samples every file
	idx_t maximum_sample_files = 1;
	//! Whether the schemas of the sampled files are combined into a union of their columns - files are then allowed
	//! to be missing columns of the combined schema, the way they are with union_by_name. This is how the schemas of
	//! sampled files are combined unless the reader combines them itself
	bool sampled_schema_is_union = true;
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
	DUCKDB_API static bool AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context);
	DUCKDB_API void AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context);
	DUCKDB_API void VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const;
	DUCKDB_API LogicalType GetHiveLogicalType(const string &hive_partition_column) const;
	DUCKDB_API Value GetHivePartitionValue(const string &base, const string &entry, ClientContext &context) const;
	DUCKDB_API bool AnySet() const;
	//! Set "maximum_sample_files" from an option value - a positive count, or -1 for all files
	DUCKDB_API void SetMaximumSampleFiles(const Identifier &key, const Value &val);
	//! Set "maximum_sample_files" from an option value, returning false if it is not a valid count
	DUCKDB_API bool TrySetMaximumSampleFiles(const Value &val);
	//! Whether the global schema is a union of the schemas of several files - individual files are then allowed to
	//! be missing columns that are present in the global schema
	bool SchemaIsUnion() const {
		return union_by_name || (maximum_sample_files > 1 && sampled_schema_is_union);
	}
};

} // namespace duckdb
