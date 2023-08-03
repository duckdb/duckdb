#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb
{
LogicalOrder::LogicalOrder(vector<BoundOrderByNode> orders)
	: LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(std::move(orders))
{
}

vector<ColumnBinding> LogicalOrder::GetColumnBindings()
{
	auto child_bindings = ((LogicalOperator*)children[0].get())->GetColumnBindings();
	if (projections.empty())
	{
		return child_bindings;
	}
	vector<ColumnBinding> result;
	for (auto &col_idx : projections)
	{
		result.push_back(child_bindings[col_idx]);
	}
	return result;
}

CKeyCollection* LogicalOrder::DeriveKeyCollection(CExpressionHandle &exprhdl)
{
	return NULL;
}
	
CPropConstraint* LogicalOrder::DerivePropertyConstraint(CExpressionHandle &exprhdl)
{
	return NULL;
}

void LogicalOrder::Serialize(FieldWriter &writer) const
{
	writer.WriteRegularSerializableList(orders);
	writer.WriteList<idx_t>(projections);
}

unique_ptr<LogicalOperator> LogicalOrder::Deserialize(LogicalDeserializationState &state, FieldReader &reader)
{
	auto orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	auto projections = reader.ReadRequiredList<idx_t>();
	auto result = make_uniq<LogicalOrder>(std::move(orders));
	result->projections = std::move(projections);
	return std::move(result);
}
} // namespace duckdb