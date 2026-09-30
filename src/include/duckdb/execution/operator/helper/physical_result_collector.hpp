//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_result_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/common/enums/statement_type.hpp"

namespace duckdb {

class PreparedStatementData;
class ColumnDataCollection;

//! PhysicalResultCollector is an abstract class that is used to generate the final result of a query
class PhysicalResultCollector : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::RESULT_COLLECTOR;

public:
	PhysicalResultCollector(PhysicalPlan &physical_plan, PreparedStatementData &data);

	StatementType statement_type;
	StatementProperties properties;
	QueryResultMemoryType memory_type;
	PhysicalOperator &plan;
	vector<Identifier> names;

public:
	static unique_ptr<PhysicalOperator> GetResultCollector(ClientContext &context, PreparedStatementData &data);

public:
	//! The final method used to fetch the query result from this operator
	virtual unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const = 0;

	bool IsSink() const override {
		return true;
	}

public:
	vector<const_reference<PhysicalOperator>> GetChildren() const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	bool IsSource() const override {
		return true;
	}

public:
	//! Whether this collector produces a result that must stay open after the fetch.
	//! Custom collectors override this to keep the query alive for their stream
	virtual bool IsStreaming() const {
		return false;
	}
	//! Whether a producer is parked on this sink and only the consumer can release it. A streaming
	//! collector without a parked-producer notion reports true: it is never waited on forever, at
	//! the price of returning to the consumer on every unrelated block
	virtual bool HasBlockedResultProducer(GlobalSinkState &state) const {
		return IsStreaming();
	}

protected:
	unique_ptr<ColumnDataCollection> CreateCollection(ClientContext &context) const;
};

} // namespace duckdb
