//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/geo_column_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "split_column_data.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/validity_column_data.hpp"

namespace duckdb {

class GeoColumnData final : public SplitColumnData {
public:
	GeoColumnData(BlockManager &block_manager, DataTableInfo &info, idx_t column_index, LogicalType type,
	              ColumnDataType data_type, optional_ptr<ColumnData> parent);

	LogicalType GetBaseType() override;

	void GetSplitTypes(vector<LogicalType> &result) override;

	void Reassemble(DataChunk &split, Vector &base) override;

	void Split(Vector &base, DataChunk &split) override;

	shared_ptr<SplitColumnData> Create(BlockManager &block_manager, DataTableInfo &info, idx_t column_index,
	                                   LogicalType type, ColumnDataType data_type,
	                                   optional_ptr<ColumnData> parent) const override;

	void CombineStats(const vector<unique_ptr<BaseStatistics>> &source_stats, BaseStatistics &target_stats) const override;

};

} // namespace duckdb
