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
		duckdb_re2::RE2 regex("[\\/\\\\]([^\\/\\?\\\\]+)=([^\\/\\n\\?\\\\]+)");
		const auto partitions = HivePartitioning::Parse(files.front(), regex);
		for (auto &f : files) {
			auto scheme = HivePartitioning::Parse(f, regex);
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

	static bool	Verify(const string& initial_file_col_name, const string& file){
		vector<string> v = StringUtil::Split(file, "=");
		if (v.size() != 2){
			return false;
		}
		if (initial_file_col_name != v.front()){
			return false;
		}
		
	}
	static std::map<string,string> Split(const vector<string>& files){
		std::map<string,string> m;
		for (auto& f : files){
			
		}
		return m;
	}
	static bool AutoDetectHivePartitioning2(const vector<string> &files) {

		if (files.empty()) {
			return false;
		}
		
		return true;
	}
};

} // namespace duckdb
