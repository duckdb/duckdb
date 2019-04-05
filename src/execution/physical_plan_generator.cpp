#include "execution/physical_plan_generator.hpp"

#include "execution/column_binding_resolver.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	// first resolve column references
	context.profiler.StartPhase("column_binding");
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	context.profiler.EndPhase();

	// now resolve types of all the operators
	context.profiler.StartPhase("resolve_types");
	op->ResolveOperatorTypes();
	context.profiler.EndPhase();

	// then create the main physical plan
	context.profiler.StartPhase("create_plan");
	auto plan = CreatePlan(*op);
	context.profiler.EndPhase();
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::GET:
		return CreatePlan((LogicalGet &)op);
	case LogicalOperatorType::PROJECTION:
		return CreatePlan((LogicalProjection &)op);
	case LogicalOperatorType::EMPTY_RESULT:
		return CreatePlan((LogicalEmptyResult &)op);
	case LogicalOperatorType::FILTER:
		return CreatePlan((LogicalFilter &)op);
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		return CreatePlan((LogicalAggregate &)op);
	case LogicalOperatorType::WINDOW:
		return CreatePlan((LogicalWindow &)op);
	case LogicalOperatorType::LIMIT:
		return CreatePlan((LogicalLimit &)op);
	case LogicalOperatorType::ORDER_BY:
		return CreatePlan((LogicalOrder &)op);
	case LogicalOperatorType::COPY:
		return CreatePlan((LogicalCopy &)op);
	case LogicalOperatorType::TABLE_FUNCTION:
		return CreatePlan((LogicalTableFunction &)op);
	case LogicalOperatorType::ANY_JOIN:
		return CreatePlan((LogicalAnyJoin &)op);
	case LogicalOperatorType::DELIM_JOIN:
		return CreatePlan((LogicalDelimJoin &)op);
	case LogicalOperatorType::COMPARISON_JOIN:
		return CreatePlan((LogicalComparisonJoin &)op);
	case LogicalOperatorType::CROSS_PRODUCT:
		return CreatePlan((LogicalCrossProduct &)op);
	case LogicalOperatorType::UNION:
	case LogicalOperatorType::EXCEPT:
	case LogicalOperatorType::INTERSECT:
		return CreatePlan((LogicalSetOperation &)op);
	case LogicalOperatorType::INSERT:
		return CreatePlan((LogicalInsert &)op);
	case LogicalOperatorType::DELETE:
		return CreatePlan((LogicalDelete &)op);
	case LogicalOperatorType::CHUNK_GET:
		return CreatePlan((LogicalChunkGet &)op);
	case LogicalOperatorType::DELIM_GET:
		return CreatePlan((LogicalDelimGet &)op);
	case LogicalOperatorType::UPDATE:
		return CreatePlan((LogicalUpdate &)op);
	case LogicalOperatorType::CREATE_TABLE:
		return CreatePlan((LogicalCreateTable &)op);
	case LogicalOperatorType::CREATE_INDEX:
		return CreatePlan((LogicalCreateIndex &)op);
	case LogicalOperatorType::EXPLAIN:
		return CreatePlan((LogicalExplain &)op);
	case LogicalOperatorType::DISTINCT:
		return CreatePlan((LogicalDistinct &)op);
	case LogicalOperatorType::PRUNE_COLUMNS:
		return CreatePlan((LogicalPruneColumns &)op);
	case LogicalOperatorType::PREPARE:
		return CreatePlan((LogicalPrepare &)op);
	case LogicalOperatorType::EXECUTE:
		return CreatePlan((LogicalExecute &)op);
	case LogicalOperatorType::INDEX_SCAN:
		return CreatePlan((LogicalIndexScan &)op);
	default:
		assert(op.type == LogicalOperatorType::SUBQUERY);
		// subquery nodes are only there for column binding; we ignore them in physical plan generation
		return CreatePlan(*op.children[0]);
	}
}
