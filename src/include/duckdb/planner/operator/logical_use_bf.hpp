#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
class LogicalCreateBF;

class LogicalUseBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USE_BF;

public:
	explicit LogicalUseBF(shared_ptr<FilterPlan> filter_plans);

	shared_ptr<FilterPlan> filter_plan;
	LogicalCreateBF *related_create_bf = nullptr;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
