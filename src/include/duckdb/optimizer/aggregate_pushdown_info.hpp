//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_pushdown_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

class Serializer;
class Deserializer;

enum class PushedAggregateType : uint8_t {
	COUNT_STAR = 0,
	COUNT_COL = 1,
	MIN = 2,
	MAX = 3
};

struct PushedAggregateInfo {
	//! The type of aggregate
	PushedAggregateType type;
	//! Column index in the underlying table
	StorageIndex col_idx;
	//! Return type of the aggregate
	LogicalType return_type;
	//! Position of this aggregate in the output DataChunk
	idx_t output_idx;
	//! Position of the source column in the deduplicated scan column_ids list
	idx_t scan_col_position = DConstants::INVALID_INDEX;
	//! The bound aggregate function.
	unique_ptr<AggregateFunction> aggregate_function;
	//! Bind data for the aggregate function
	unique_ptr<FunctionData> bind_data;

	void Serialize(Serializer &serializer) const;
	static PushedAggregateInfo Deserialize(Deserializer &deserializer);
};

struct AggregatePushdownInfo {
	//! The pushed-down aggregates, in output order
	vector<PushedAggregateInfo> aggregates;

	vector<LogicalType> GetOutputTypes() const {
		vector<LogicalType> types;
		types.reserve(aggregates.size());
		for (auto &a : aggregates) {
			types.push_back(a.return_type);
		}
		return types;
	}

	void Serialize(Serializer &serializer) const;
	static unique_ptr<AggregatePushdownInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
