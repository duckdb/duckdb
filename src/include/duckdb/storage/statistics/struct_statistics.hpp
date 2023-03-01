//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/struct_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

namespace duckdb {
class Value;

class StructStatistics : public BaseStatistics {
public:
	explicit StructStatistics(LogicalType type);

public:
	void Merge(const BaseStatistics &other) override;
	FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const Value &constant) const;

	unique_ptr<BaseStatistics> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<BaseStatistics> Deserialize(FieldReader &reader, LogicalType type);
	void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const override;

	string ToString() const override;

	BaseStatistics &GetChildStats(idx_t i) {
		if (i >= child_stats.size() || !child_stats[i]) {
			throw InternalException("Calling StructStatistics::GetChildStats but there are no stats for this index");
		}
		return *child_stats[i];
	}
	void SetChildStats(idx_t i, unique_ptr<BaseStatistics> stats) {
		child_stats[i] = std::move(stats);
	}
	vector<unique_ptr<BaseStatistics>> &GetChildStats() {
		return child_stats;
	}

private:
	vector<unique_ptr<BaseStatistics>> child_stats;
};

} // namespace duckdb
