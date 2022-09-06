//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

//! LogicalOrder represents an ORDER BY clause, sorting the data
class LogicalOrder : public LogicalOperator {
public:
	explicit LogicalOrder(vector<BoundOrderByNode> orders)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(move(orders)) {
	}

	vector<BoundOrderByNode> orders;
	vector<unique_ptr<Expression>> projections;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	string ParamsToString() const override {
		string result;
		for (idx_t i = 0; i < orders.size(); i++) {
			if (i > 0) {
				result += "\n";
			}
			result += orders[i].expression->GetName();
		}
		return result;
	}

protected:
	void ResolveTypes() override {
		for (auto &proj : projections) {
			types.push_back(proj->return_type);
		}
	}
};
} // namespace duckdb
