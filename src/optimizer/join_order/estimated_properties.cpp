
#include "duckdb/optimizer/join_order/estimated_properties.hpp"

namespace duckdb {

template <>
double EstimatedProperties::GetCardinality() const {
	return cardinality;
}

template <>
idx_t EstimatedProperties::GetCardinality() const {
	auto max_idx_t = NumericLimits<idx_t>::Maximum() - 10000;
	return MinValue<double>(cardinality, max_idx_t);
}

template <>
double EstimatedProperties::GetCost() const {
	return cost;
}

template <>
idx_t EstimatedProperties::GetCost() const {
	auto max_idx_t = NumericLimits<idx_t>::Maximum() - 10000;
	return MinValue<double>(cost, max_idx_t);
}

void EstimatedProperties::SetCardinality(double new_card) {
	cardinality = new_card;
}

void EstimatedProperties::SetCost(double new_cost) {
	cost = new_cost;
}

} // namespace duckdb
