//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group_reorderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/partition_stats.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {

enum class OrderByStatistics : uint8_t { MIN, MAX };
enum class OrderByColumnType : uint8_t { NUMERIC, STRING };

struct RowGroupOrderOptions {
	RowGroupOrderOptions(const StorageIndex &column_idx_p, OrderByStatistics order_by_p, OrderType order_type_p,
	                     OrderByColumnType column_type_p, optional_idx row_limit_p = optional_idx(),
	                     idx_t row_group_offset_p = 0)
	    : column_idx(column_idx_p), order_by(order_by_p), order_type(order_type_p), column_type(column_type_p),
	      row_limit(row_limit_p), row_group_offset(row_group_offset_p) {
	}

	const StorageIndex column_idx;
	const OrderByStatistics order_by;
	const OrderType order_type;
	const OrderByColumnType column_type;
	const optional_idx row_limit;
	const idx_t row_group_offset;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<RowGroupOrderOptions> Deserialize(Deserializer &deserializer);
};

struct OffsetPruningResult {
	idx_t offset_remainder;
	idx_t pruned_row_group_count;
};

class RowGroupReorderer {
public:
	explicit RowGroupReorderer(const RowGroupOrderOptions &options_p);
	optional_ptr<SegmentNode<RowGroup>> GetRootSegment(RowGroupSegmentTree &row_groups);
	optional_ptr<SegmentNode<RowGroup>> GetNextRowGroup(SegmentNode<RowGroup> &row_group);

	static Value RetrieveStat(const BaseStatistics &stats, OrderByStatistics order_by, OrderByColumnType column_type);
	static OffsetPruningResult GetOffsetAfterPruning(OrderByStatistics order_by, OrderByColumnType column_type,
	                                                 OrderType order_type, const StorageIndex &column_idx,
	                                                 idx_t row_offset, vector<PartitionStatistics> &stats);

private:
	const RowGroupOrderOptions options;

	idx_t offset;
	bool initialized;
	vector<reference<SegmentNode<RowGroup>>> ordered_row_groups;
};

} // namespace duckdb
