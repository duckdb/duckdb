#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_bf.hpp"

namespace duckdb {
class LogicalUseBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USE_BF;

public:
	explicit LogicalUseBF(vector<shared_ptr<BlockedBloomFilter>> bloom_filters);
	explicit LogicalUseBF(shared_ptr<BlockedBloomFilter> bloom_filter);

	vector<shared_ptr<BlockedBloomFilter>> bf_to_use;
	vector<LogicalCreateBF *> related_create_bf;

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
