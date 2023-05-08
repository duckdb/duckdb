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
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
struct BindInfo;

struct MultiFileReaderOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool auto_detect_hive_partitioning = true;
	bool union_by_name = false;
	bool hive_types = false;
	case_insensitive_map_t<LogicalType> hive_types_schema;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderOptions Deserialize(Deserializer &source);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info) const;

	void AutoDetect(const vector<string> &files, ClientContext& context) {
		if (!auto_detect_hive_partitioning && !hive_partitioning && hive_types) {
			throw InvalidInputException("cannot disable hive_partitioning when using hive_types");
		}
		if (files.empty()) {
			return;
		}
		if (auto_detect_hive_partitioning) {
			hive_partitioning = AutoDetectHivePartitioning(files);
		}
		if (hive_partitioning && !hive_types) {
			hive_types = AutoDetectHiveTypes(files.front(), context);
		}
	}

	static bool AutoDetectHivePartitioning(const vector<string> &files) {
		std::unordered_set<string> uset;
		idx_t splits_size;
		{
			//	front file
			auto splits = StringUtil::Split(files.front(), FileSystem::PathSeparator());
			splits_size = splits.size();
			if (splits.size() < 2) {
				return false;
			}
			for (auto it = splits.begin(); it != std::prev(splits.end()); it++) {
				auto part = StringUtil::Split(*it, "=");
				if (part.size() == 2) {
					uset.insert(part.front());
				}
			}
		}
		if (uset.empty()) {
			return false;
		}
		for (auto &file : files) {
			auto splits = StringUtil::Split(file, FileSystem::PathSeparator());
			if (splits.size() != splits_size) {
				return false;
			}
			for (auto it = splits.begin(); it != std::prev(splits.end()); it++) {
				auto part = StringUtil::Split(*it, "=");
				if (part.size() == 2) {
					if (uset.find(part.front()) == uset.end()) {
						return false;
					}
				}
			}
		}
		return true;
	}

	bool AutoDetectHiveTypes(const string &file, ClientContext& context) {
		std::map<string,string> partitions;
		auto splits = StringUtil::Split(file, FileSystem::PathSeparator());
		if (splits.size() < 2) {
			return false;
		}
		for (auto it = splits.begin(); it != std::prev(splits.end()); it++) {
			auto part = StringUtil::Split(*it, "=");
			if (part.size() == 2) {
				partitions[part.front()] = part.back();
			}
		}
		if (partitions.empty()) {
			return false;
		}

		// do the actual detection
		const LogicalType candidates[] = {LogicalType::DATE,LogicalType::TIMESTAMP,LogicalType::BIGINT};
		for (auto& child : partitions) {
			const string& name = child.first;
			const string& type = child.second;
			Value value(type);
			for (auto& candidate : candidates) {
				const bool success = value.TryCastAs(context, candidate);
				if (success) {
					hive_types_schema[name] = candidate;
					break;
				}
			}
		}
		return !hive_types_schema.empty();
	}

	LogicalType GetHiveLogicalType(const string &hive_partition_column) const {
		if (hive_types) {
			auto it = hive_types_schema.find(hive_partition_column);
			if (it != hive_types_schema.end()) {
				return it->second;
			}
		}
		return LogicalType::VARCHAR;
	}
};

} // namespace duckdb
