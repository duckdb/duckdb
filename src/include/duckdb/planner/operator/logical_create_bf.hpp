#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/operator/persistent/physical_create_bf.hpp"

namespace duckdb {
class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

public:
	explicit LogicalCreateBF(vector<shared_ptr<BlockedBloomFilter>> bloom_filters);

	PhysicalCreateBF *physical = nullptr;
	vector<shared_ptr<BlockedBloomFilter>> bf_to_create;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
