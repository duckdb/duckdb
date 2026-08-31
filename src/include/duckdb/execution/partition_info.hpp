//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/partition_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

enum class PartitionInfo { NONE, REQUIRES_BATCH_INDEX };

struct ColumnPartitionData {
	explicit ColumnPartitionData(Value partition_val) : min_val(partition_val), max_val(std::move(partition_val)) {
	}

	Value min_val;
	Value max_val;
};

struct SourcePartitionInfo {
	//! The current batch index
	//! This is only set in case RequiresBatchIndex() is true, and the source has support for it (SupportsBatchIndex())
	//! Otherwise this is left on INVALID_INDEX
	//! The batch index is a globally unique, increasing index that should be used to maintain insertion order
	//! //! in conjunction with parallelism
	optional_idx batch_index;
	//! The minimum batch index that any thread is currently actively reading
	optional_idx min_batch_index;
	//! Column partition data
	vector<ColumnPartitionData> partition_data;
};

struct OperatorPartitionInfo {
	OperatorPartitionInfo() = default;
	explicit OperatorPartitionInfo(bool batch_index) : batch_index(batch_index) {
	}
	OperatorPartitionInfo(bool batch_index, optional_idx preferred_batch_size)
	    : batch_index(batch_index), preferred_batch_size(preferred_batch_size) {
	}
	explicit OperatorPartitionInfo(vector<column_t> partition_columns_p)
	    : partition_columns(std::move(partition_columns_p)) {
	}

	bool batch_index = false;
	optional_idx preferred_batch_size;
	vector<column_t> partition_columns;

	static OperatorPartitionInfo NoPartitionInfo() {
		return OperatorPartitionInfo(false);
	}
	static OperatorPartitionInfo BatchIndex(optional_idx preferred_batch_size = optional_idx()) {
		return OperatorPartitionInfo(true, preferred_batch_size);
	}
	static OperatorPartitionInfo PartitionColumns(vector<column_t> columns) {
		return OperatorPartitionInfo(std::move(columns));
	}
	bool RequiresPartitionColumns() const {
		return !partition_columns.empty();
	}
	bool RequiresBatchIndex() const {
		return batch_index;
	}
	bool AnyRequired() const {
		return RequiresPartitionColumns() || RequiresBatchIndex();
	}
};

struct OperatorPartitionData {
	explicit OperatorPartitionData(idx_t batch_index) : batch_index(batch_index) {
	}

	idx_t batch_index;
	vector<ColumnPartitionData> partition_data;
};

} // namespace duckdb
