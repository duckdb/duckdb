//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_bf.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/predicate_transfer/dag.hpp"

namespace duckdb {
class PhysicalCreateBF;

class LogicalCreateBF : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_BF;

public:
	explicit LogicalCreateBF(vector<shared_ptr<FilterPlan>> filter_plans);

	bool is_probing_side = false;
	vector<shared_ptr<FilterPlan>> filter_plans;
	PhysicalCreateBF *physical = nullptr;

	vector<shared_ptr<DynamicTableFilterSet>> min_max_to_create;
	vector<vector<ColumnBinding>> min_max_applied_cols;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
