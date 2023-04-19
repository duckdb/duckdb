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

#include <iostream>
#include <iomanip>
#define LINE std::cerr << "line: " << __LINE__ << std::endl;
#define FL   std::cerr << __FUNCTION__ << ":" << __LINE__ << std::endl;
#define FFL  std::cerr << __FILE__ << ":" << __FUNCTION__ << ":" << __LINE__ << std::endl;
#define VAR(var)                                                                                                       \
	std::cerr << std::boolalpha << __LINE__ << ":\t" << (#var) << " = [" << (var) << "]" << std::noboolalpha           \
	          << std::endl;
template <typename T>
static void print(const T &c, bool newlines = true) {
	const char delim = newlines ? '\n' : ' ';
	idx_t i = 0;
	for (auto it = c.begin(); it != c.end(); it++)
		std::cerr << "[" << std::setw(log10(c.size()) + 1) << i++ << "] [" << *it << "]" << delim;
	if (!newlines)
		std::cerr << std::endl;
}

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

		// FL
		// print(files);

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
