//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/zone_map_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"

namespace duckdb {



// Conjunction OR filters and Zone maps are tough to optimize


/* The main problem is that if a column is not sorted, then there is a chance every value in the
 * conjunction or is in every row group. We would like to avoid this.
 *
 * Also, per row group, we are checking the min and the max of the row group to see if our OR values
 * may be contained in the row group. Again if the row group is not sorted, then we may be performing this
 * check unnecessarily.
 *
 * Example.
 * Every row group have the table min and max. This means we cannot skip any row groups
 *
 * If the values are sorted, then there is a good chance we can prune row groups since the
 * min and max values of the row groups will be increasing.
 *
 * Lets go case by case
 * HIGH cardinality >> LOW distinct count, then not worth it to do zone maps. Most likely every row group will be propagated
 * HIGH cardinality ~~ HIGH distinct count. Worth it to do
 * LOW cardinality ~~ LOW distinct count (Not worth it. You may filter out a good number of row groups, but checking
 * each one for min max statistics is not worth the overhead.
 *
 * LOW CARDINALITY - HIGH DISTINCT COUNT - impossible.
 *
 */

class ZoneMapFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::ZONE_MAP;


private:

public:
	// zonemap filter
	unique_ptr<TableFilter> child_filter;

public:
	ZoneMapFilter();

	string ToString(const string &column_name) override;
	unique_ptr<TableFilter> Copy() const override;
	unique_ptr<Expression> ToExpression(const Expression &column) const override;
	FilterPropagateResult CheckStatistics(BaseStatistics &stats) override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
