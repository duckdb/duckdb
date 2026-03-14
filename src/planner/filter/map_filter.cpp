#include "duckdb/planner/filter/map_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

MapFilter::MapFilter(Value key, unique_ptr<TableFilter> child_filter)
    : TableFilter(TableFilterType::MAP_EXTRACT), key(std::move(key)), child_filter(std::move(child_filter)) {
}

FilterPropagateResult MapFilter::CheckStatistics(BaseStatistics &stats) const {
	D_ASSERT(stats.GetType().id() == LogicalTypeId::MAP);

	// MAP is internally represented as LIST<STRUCT<key, value>>
	// Get the LIST child statistics (which contains the STRUCT)
	auto &list_stats = ListStats::GetChildStats(stats);
	if (list_stats.GetType().id() != LogicalTypeId::STRUCT) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	// Get the STRUCT statistics which has two fields: key (index 0) and value (index 1)
	D_ASSERT(StructType::GetChildCount(list_stats.GetType()) == 2);
	auto &value_stats = StructStats::GetChildStats(list_stats, 1);

	// Check the child filter against the value statistics
	auto result = child_filter->CheckStatistics(value_stats);

	// Key insight: Statistics are aggregated across ALL keys in the row group.
	// - If FILTER_ALWAYS_FALSE: predicate is outside [min, max] range for ALL keys
	//   → Safe to skip: no key (including ours) can satisfy the predicate
	// - If FILTER_ALWAYS_TRUE or NO_PRUNING_POSSIBLE: predicate overlaps the range
	//   → Cannot determine if OUR specific key satisfies it
	//   → Must read the row group

	if (result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		// Predicate is outside the [min, max] range of all values
		// Therefore, our specific key cannot satisfy it either
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}

	// For all other cases, we cannot make assumptions about the specific key
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string MapFilter::ToString(const string &column_name) const {
	return child_filter->ToString("element_at(" + column_name + ", " + key.ToString() + ")");
}

bool MapFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<MapFilter>();
	return key == other.key && child_filter->Equals(*other.child_filter);
}

unique_ptr<TableFilter> MapFilter::Copy() const {
	return make_uniq<MapFilter>(key, child_filter->Copy());
}

unique_ptr<Expression> MapFilter::ToExpression(const Expression &column) const {
	// ToExpression is used to convert the filter back into an expression for some optimization passes
	// For MAP filters, this is complex because we need to reconstruct the map_extract_value call
	// For now, we return nullptr which indicates this filter cannot be converted back to an expression
	// This is acceptable as the filter will still work for its primary purpose (row group skipping)
	return nullptr;
}

void MapFilter::Serialize(Serializer &serializer) const {
	TableFilter::Serialize(serializer);
	serializer.WriteProperty(200, "key", key);
	serializer.WriteProperty(201, "child_filter", child_filter);
}

unique_ptr<TableFilter> MapFilter::Deserialize(Deserializer &deserializer) {
	auto key = deserializer.ReadProperty<Value>(200, "key");
	auto child_filter = deserializer.ReadProperty<unique_ptr<TableFilter>>(201, "child_filter");
	return make_uniq<MapFilter>(std::move(key), std::move(child_filter));
}

} // namespace duckdb
