#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
class LogicalUseBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USE_BF;

public:
	explicit LogicalUseBF(shared_ptr<BloomFilterPlan> bloom_filter);
	explicit LogicalUseBF(vector<shared_ptr<BloomFilterPlan>> bloom_filter_plans);

	vector<shared_ptr<BloomFilterPlan>> bf_to_use_plans;
	vector<LogicalCreateBF *> related_create_bfs;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<ColumnBinding> GetColumnBindings() override;

public:
	void AddDownStreamOperator(LogicalCreateBF *op);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
