//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalExecute : public LogicalOperator {
public:
	explicit LogicalExecute(shared_ptr<PreparedStatementData> prepared_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXECUTE), prepared(std::move(prepared_p)) {
		D_ASSERT(prepared);
		types = prepared->types;
	}

	shared_ptr<PreparedStatementData> prepared;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		// already resolved
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> bindings;
		for (idx_t i = 0; i < types.size(); i++) {
			bindings.push_back(ColumnBinding(0, i));
		}
		return bindings;
	}
};
} // namespace duckdb
