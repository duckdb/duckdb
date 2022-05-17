//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_result_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalResultCollector is an abstract class that is used to generate the final result of a query
class PhysicalResultCollector : public PhysicalOperator {
public:
	PhysicalResultCollector(PhysicalOperator *plan);

	PhysicalOperator *plan;

public:
	//! The final method used to fetch the query result from this operator
	virtual unique_ptr<QueryResult> GetResult(GlobalSinkState &state) = 0;

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
};

} // namespace duckdb
