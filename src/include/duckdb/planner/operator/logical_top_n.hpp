//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_top_n.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
struct DynamicFilterData;

//! LogicalTopN represents a comibination of ORDER BY and LIMIT clause, using Min/Max Heap
class LogicalTopN : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TOP_N;

public:
	LogicalTopN(vector<BoundOrderByNode> orders, idx_t limit, idx_t offset);
	~LogicalTopN() override;

	vector<BoundOrderByNode> orders;
	//! The maximum amount of elements to emit
	idx_t limit;
	//! The offset from the start to begin emitting elements
	idx_t offset;
	//! Dynamic table filter (if any)
	shared_ptr<DynamicFilterData> dynamic_filter;
	vector<ProjectionIndex> projection_map;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		auto child_bindings = children[0]->GetColumnBindings();
		if (!HasProjectionMap()) {
			return child_bindings;
		}
		return MapBindings(child_bindings, projection_map);
	}

	bool HasProjectionMap() const override {
		return !projection_map.empty();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		const auto child_types = children[0]->types;
		if (!HasProjectionMap()) {
			types = child_types;
		} else {
			types = MapTypes(child_types, projection_map);
		}
	}
};
} // namespace duckdb
