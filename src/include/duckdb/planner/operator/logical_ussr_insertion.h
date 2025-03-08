//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_set_operation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalUSSRInsertion : public LogicalOperator {

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_USSR_INSERTION;

	explicit LogicalUSSRInsertion(vector<bool> cols_to_insert)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_USSR_INSERTION), insert_to_ussr(std::move(cols_to_insert)) {
	}

	vector<bool> insert_to_ussr;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
