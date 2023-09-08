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

//! LogicalTopN represents a comibination of ORDER BY and LIMIT clause, using Min/Max Heap
class LogicalTopN : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TOP_N;

public:
	LogicalTopN(vector<BoundOrderByNode> orders, int64_t limit, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N), orders(std::move(orders)), limit(limit), offset(offset) {
	}

	vector<BoundOrderByNode> orders;
	//! The maximum amount of elements to emit
	int64_t limit;
	//! The offset from the start to begin emitting elements
	int64_t offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
