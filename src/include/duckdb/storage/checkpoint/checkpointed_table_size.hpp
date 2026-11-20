//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/checkpoint/checkpointed_table_size.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
class ClientContext;
struct DataTableInfo;

//! The column layout and row group directory of one durable table snapshot.
class CheckpointedTableSize {
public:
	CheckpointedTableSize(MetaBlockPointer directory, idx_t row_group_count, vector<LogicalType> column_types);
	optional_idx Estimate(ClientContext &context, DataTableInfo &table, idx_t sample_row_groups);

	//! Keep a single sample with selection probability proportional to the number of rows.
	static bool SelectRowGroup(RandomEngine &random, double weight, double &total_weight) {
		D_ASSERT(weight >= 0 && total_weight >= 0);
		if (weight == 0) {
			return false;
		}
		const bool first = total_weight == 0;
		total_weight += weight;
		return first || random.NextRandom() < weight / total_weight;
	}

private:
	optional_idx Sample(ClientContext &context, DataTableInfo &table, idx_t sample_count) const;

	MetaBlockPointer directory;
	idx_t row_group_count;
	vector<LogicalType> column_types;
	mutex cache_lock;
	idx_t cached_sample_count = 0;
	optional_idx cached_bytes;
};

} // namespace duckdb
