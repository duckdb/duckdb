//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "re2/re2.h"

namespace duckdb {
class Serializer;
class Deserializer;
struct BindInfo;

struct MultiFileReaderOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool auto_detect_hive_partitioning = true;
	bool union_by_name = false;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static MultiFileReaderOptions Deserialize(Deserializer &source);
	DUCKDB_API void AddBatchInfo(BindInfo &bind_info) const;

	static bool AutoDetectHivePartitioning(const vector<string> &files) {
		if (files.empty()) {
			return false;
		}

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
};

} // namespace duckdb
