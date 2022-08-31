//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/estimated_properties.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/storage/statistics/distinct_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class EstimatedProperties {
public:
	EstimatedProperties(double cardinality, double cost) : cardinality(cardinality), cost(cost) {};
	EstimatedProperties() : cardinality(0), cost(0) {};

	double GetCardinality();
	double GetCost();
	void SetCost(double new_cost);
	void SetCardinality(double cardinality);

private:
	double cardinality;
	double cost;

public:
	unique_ptr<EstimatedProperties> Copy();
};
} // namespace duckdb
