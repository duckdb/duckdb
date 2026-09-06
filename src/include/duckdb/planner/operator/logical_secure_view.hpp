//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_secure_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSecureView wraps the expanded plan of a secure view. It does not alter the result of its child - it only
//! acts as an optimization barrier that prevents the optimizer from pushing anything into the view.
class LogicalSecureView : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_SECURE_VIEW;

public:
	LogicalSecureView(string view_name, unique_ptr<LogicalOperator> child);

	//! The name of the view - used for printing the plan
	string view_name;

public:
	vector<ColumnBinding> GetColumnBindings() override;
	idx_t EstimateCardinality(ClientContext &context) override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;

private:
	LogicalSecureView();
};

} // namespace duckdb
