//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_tokens.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
class ClientContext;
class ColumnDataCollection;

//! The physical plan generator generates a physical execution plan from a
//! logical query plan
class PhysicalPlanGenerator {
public:
	explicit PhysicalPlanGenerator(ClientContext &context);
	~PhysicalPlanGenerator();

	DependencyList dependencies;
	//! Recursive CTEs require at least one ChunkScan, referencing the working_table.
	//! This data structure is used to establish it.
	unordered_map<idx_t, std::shared_ptr<ColumnDataCollection>> recursive_cte_tables;
	//! Materialized CTE ids must be collected.
	unordered_set<idx_t> materialized_ctes;

public:
	//! Creates a plan from the logical operator. This involves resolving column bindings and generating physical
	//! operator nodes.
	unique_ptr<PhysicalOperator> CreatePlan(unique_ptr<LogicalOperator> logical);

	//! Whether or not we can (or should) use a batch-index based operator for executing the given sink
	static bool UseBatchIndex(ClientContext &context, PhysicalOperator &plan);
	//! Whether or not we should preserve insertion order for executing the given sink
	static bool PreserveInsertionOrder(ClientContext &context, PhysicalOperator &plan);

protected:
	unique_ptr<PhysicalOperator> CreatePlan(LogicalOperator &op);

	unique_ptr<PhysicalOperator> CreatePlan(LogicalAggregate &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalAnyJoin &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalColumnDataGet &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalComparisonJoin &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreate &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateTable &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCreateIndex &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCrossProduct &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDelete &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalDelimGet &op);
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
	unique_ptr<PhysicalOperator> CreatePlan(LogicalPositionalJoin &op);
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
	unique_ptr<PhysicalOperator> CreatePlan(LogicalReset &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalShow &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalSimple &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalUnnest &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalRecursiveCTE &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalMaterializedCTE &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalCTERef &op);
	unique_ptr<PhysicalOperator> CreatePlan(LogicalPivot &op);

	unique_ptr<PhysicalOperator> PlanAsOfJoin(LogicalComparisonJoin &op);
	unique_ptr<PhysicalOperator> PlanComparisonJoin(LogicalComparisonJoin &op);
	unique_ptr<PhysicalOperator> PlanDelimJoin(LogicalComparisonJoin &op);
	unique_ptr<PhysicalOperator> ExtractAggregateExpressions(unique_ptr<PhysicalOperator> child,
	                                                         vector<unique_ptr<Expression>> &expressions,
	                                                         vector<unique_ptr<Expression>> &groups);

private:
	bool PreserveInsertionOrder(PhysicalOperator &plan);
	bool UseBatchIndex(PhysicalOperator &plan);

private:
	ClientContext &context;
};
} // namespace duckdb
