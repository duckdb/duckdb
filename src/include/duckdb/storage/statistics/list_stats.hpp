//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/list_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hugeint.hpp"

namespace duckdb {
class BaseStatistics;
class FieldWriter;
class FieldReader;
struct SelectionVector;
class Vector;

struct ListStats {
	DUCKDB_API static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	//! Whether or not the statistics are for a list
	DUCKDB_API static bool IsList(const BaseStatistics &stats);

	DUCKDB_API static const unique_ptr<BaseStatistics> &GetChildStats(const BaseStatistics &stats);
	DUCKDB_API static unique_ptr<BaseStatistics> &GetChildStats(BaseStatistics &stats);

	DUCKDB_API static void Serialize(const BaseStatistics &stats, FieldWriter &writer);
	DUCKDB_API static unique_ptr<BaseStatistics> Deserialize(FieldReader &reader, LogicalType type);

	DUCKDB_API static string ToString(const BaseStatistics &stats);

	DUCKDB_API static void Merge(BaseStatistics &stats, const BaseStatistics &other);
	DUCKDB_API static void Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count);
};

} // namespace duckdb
