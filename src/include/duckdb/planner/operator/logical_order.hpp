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
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ORDER_BY;

public:
	explicit LogicalOrder(vector<BoundOrderByNode> orders);

	vector<BoundOrderByNode> orders;

	vector<idx_t> projections;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	CKeyCollection* DeriveKeyCollection(CExpressionHandle &exprhdl) override;
	
	CPropConstraint* DerivePropertyConstraint(CExpressionHandle &exprhdl) override;

	void Serialize(FieldWriter &writer) const override;
	
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

	string ParamsToString() const override
	{
		string result = "ORDERS:\n";
		for (idx_t i = 0; i < orders.size(); i++)
		{
			if (i > 0) {
				result += "\n";
			}
			result += orders[i].expression->GetName();
		}
		return result;
	}

protected:
	void ResolveTypes() override {
		const auto child_types = children[0]->types;
		if (projections.empty()) {
			types = child_types;
		} else {
			for (auto &col_idx : projections) {
				types.push_back(child_types[col_idx]);
			}
		}
	}
};
} // namespace duckdb
