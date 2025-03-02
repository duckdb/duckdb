#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

public:
	explicit LogicalCreateBF(vector<shared_ptr<BloomFilterPlan>> bloom_filters);

	vector<shared_ptr<BloomFilterPlan>> bf_to_create_plans;
	PhysicalCreateBF *physical = nullptr;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
