//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/filename_pattern.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_COPY_TO_FILE;

public:
	LogicalCopyToFile(CopyFunction function, unique_ptr<FunctionData> bind_data, unique_ptr<CopyInfo> copy_info)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_TO_FILE), function(std::move(function)),
	      bind_data(std::move(bind_data)), copy_info(std::move(copy_info)) {
	}
	CopyFunction function;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<CopyInfo> copy_info;

	std::string file_path;
	bool use_tmp_file;
	FilenamePattern filename_pattern;
	string file_extension;
	bool overwrite_or_ignore;
	bool per_thread_output;
	optional_idx file_size_bytes;

	bool partition_output;
	vector<idx_t> partition_columns;
	vector<string> names;
	vector<LogicalType> expected_types;

public:
	idx_t EstimateCardinality(ClientContext &context) override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
