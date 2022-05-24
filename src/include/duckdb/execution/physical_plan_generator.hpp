//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"

namespace duckdb {
class ClientContext;

//! The physical plan generator generates a physical execution plan from a
//! logical query plan
class PhysicalPlanGenerator {
public:
	explicit PhysicalPlanGenerator(ClientContext &context) : context(context) {
	}

	unordered_set<CatalogEntry *> dependencies;
	//! Recursive CTEs require at least one ChunkScan, referencing the working_table.
	//! This data structure is used to establish it.
	unordered_map<idx_t, std::shared_ptr<ChunkCollection>> rec_ctes;

public:
	//! Creates a plan from the logical operator. This involves resolving column bindings and generating physical
	//! operator nodes.
	unique_ptr<PhysicalOperator> CreatePlan(unique_ptr<LogicalOperator> logical);

protected:
	unique_ptr<PhysicalOperator> CreatePlan(LogicalOperator &op);

	unique_ptr<PhysicalOperator> CreatePlan(LogicalAggregate &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalAnyJoin &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalChunkGet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalComparisonJoin &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreate &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateTable &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateMatView &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateIndex &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCrossProduct &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDelete &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDelimGet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDelimJoin &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDistinct &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDummyScan &expr);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalEmptyResult &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalExpressionGet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalExport &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalFilter &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalGet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalLimit &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalLimitPercent &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalOrder &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalTopN &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalProjection &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalInsert &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCopyToFile &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalExplain &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalSetOperation &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalUpdate &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalPrepare &expr);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalWindow &expr);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalExecute &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalPragma &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalSample &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalSet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalShow &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalSimple &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalUnnest &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalRecursiveCTE &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCTERef &op);

	unique_ptr<PhysicalOperator> CreateDistinctOn(unique_ptr<PhysicalOperator> child,
	                                              vector<unique_ptr<Expression>> distinct_targets);

	unique_ptr<PhysicalOperator> ExtractAggregateExpressions(unique_ptr<PhysicalOperator> child,
	                                                         vector<unique_ptr<Expression>> &expressions,
	                                                         vector<unique_ptr<Expression>> &groups);

private:
	ClientContext &context;
};
} // namespace duckdb
