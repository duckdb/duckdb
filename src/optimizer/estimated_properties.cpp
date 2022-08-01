
#include "duckdb/optimizer/estimated_properties.hpp"

namespace duckdb {

double EstimatedProperties::GetCardinality() {
	return cardinality;
}
double EstimatedProperties::GetCost() {
	return cost;
}

void EstimatedProperties::SetCardinality(double new_card) {
	cardinality = new_card;
}

} // namespace duckdb
