//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/zone_map_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

class ZoneMapFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::ZONE_MAP;

public:
	// zonemap filter
	unique_ptr<TableFilter> child_filter;

	ZoneMapFilter() : TableFilter(TableFilterType::ZONE_MAP) {
	}

	string ToString(const string &column_name) override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

	~ZoneMapFilter() override {
	}
};

} // namespace duckdb
