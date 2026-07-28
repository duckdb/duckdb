//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/extra_operator_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

class ExtraOperatorInfo {
public:
	ExtraOperatorInfo() : file_filters(""), sample_options(nullptr) {
	}
	ExtraOperatorInfo(ExtraOperatorInfo &&extra_info) noexcept
	    : file_filters(std::move(extra_info.file_filters)), sample_options(std::move(extra_info.sample_options)) {
		if (extra_info.total_files.IsValid()) {
			total_files = extra_info.total_files.GetIndex();
		}
		if (extra_info.filtered_files.IsValid()) {
			filtered_files = extra_info.filtered_files.GetIndex();
		}
		if (extra_info.pruned_directories.IsValid()) {
			pruned_directories = extra_info.pruned_directories.GetIndex();
		}
	}
	ExtraOperatorInfo &operator=(ExtraOperatorInfo &&extra_info) noexcept {
		if (this != &extra_info) {
			file_filters = extra_info.file_filters;
			if (extra_info.total_files.IsValid()) {
				total_files = extra_info.total_files.GetIndex();
			}
			if (extra_info.filtered_files.IsValid()) {
				filtered_files = extra_info.filtered_files.GetIndex();
			}
			if (extra_info.pruned_directories.IsValid()) {
				pruned_directories = extra_info.pruned_directories.GetIndex();
			}
			sample_options = std::move(extra_info.sample_options);
		}
		return *this;
	}

	bool operator==(const ExtraOperatorInfo &other) const {
		return file_filters == other.file_filters && total_files == other.total_files &&
		       filtered_files == other.filtered_files && pruned_directories == other.pruned_directories &&
		       sample_options == other.sample_options;
	}

	//! Filters that have been pushed down into the main file list
	string file_filters;
	//! Total size of file list - only counts the files that were expanded, directories that were pruned before they
	//! were listed are not part of this
	optional_idx total_files;
	//! Size of file list after applying filters
	optional_idx filtered_files;
	//! Number of directories that were skipped without being listed because their hive partition keys could not match
	optional_idx pruned_directories;
	//! Sample options that have been pushed down into the table scan
	unique_ptr<SampleOptions> sample_options;

	void Serialize(Serializer &serializer) const;
	static ExtraOperatorInfo Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
