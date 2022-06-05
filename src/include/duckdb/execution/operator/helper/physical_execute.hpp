//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

class PhysicalExecute : public PhysicalOperator {
public:
	explicit PhysicalExecute(PhysicalOperator *plan);

	PhysicalOperator *plan;
	unique_ptr<PhysicalOperator> owned_plan;
	shared_ptr<PreparedStatementData> prepared;

public:
	vector<PhysicalOperator *> GetChildren() const override;

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
};

} // namespace duckdb
