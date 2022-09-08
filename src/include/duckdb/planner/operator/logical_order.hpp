//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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

	idx_t table_index;
	vector<unique_ptr<Expression>> projections;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		auto child_bindings = children[0]->GetColumnBindings();
		if (projections.empty()) {
			return child_bindings;
		}

		vector<ColumnBinding> result;
		for (idx_t i = 0; i < projections.size(); i++) {
			result.emplace_back(table_index, i);
		}
		return result;
	}

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	string ParamsToString() const override {
		string result = "ORDERS:\n";
		for (idx_t i = 0; i < orders.size(); i++) {
			if (i > 0) {
				result += "\n";
			}
			result += orders[i].expression->GetName();
		}
		result += "\nPROJECTIONS:\n";
		for (idx_t i = 0; i < projections.size(); i++) {
			if (i > 0) {
				result += "\n";
			}
			result += projections[i]->GetName();
		}
		return result;
	}

protected:
	void ResolveTypes() override {
		if (projections.empty()) {
			types = children[0]->types;
		} else {
			for (auto &proj : projections) {
				types.push_back(proj->return_type);
			}
		}
	}
};
} // namespace duckdb
