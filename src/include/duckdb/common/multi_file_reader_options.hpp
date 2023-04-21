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
		const auto partitions = HivePartitioning::Parse(files.front());
		for (auto &f : files) {
			auto scheme = HivePartitioning::Parse(f);
			if (scheme.size() != partitions.size()) {
				return false;
			}
			for (auto &i : scheme) {
				if (partitions.find(i.first) == partitions.end()) {
					return false;
				}
			}
		}
		return true;
	}
};

} // namespace duckdb
