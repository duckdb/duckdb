#include "duckdb/execution/physical_plan_generator.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	DependencyExtractor(unordered_set<CatalogEntry *> &dependencies) : dependencies(dependencies) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		// extract dependencies from the bound function expression
		if (expr.function.dependency) {
			expr.function.dependency(expr, dependencies);
		}
		return nullptr;
	}

private:
	unordered_set<CatalogEntry *> &dependencies;
};
} // namespace duckdb

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

	// extract dependencies from the logical plan
	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(*op);

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
	case LogicalOperatorType::UNNEST:
		return CreatePlan((LogicalUnnest &)op);
	case LogicalOperatorType::LIMIT:
		return CreatePlan((LogicalLimit &)op);
	case LogicalOperatorType::ORDER_BY:
		return CreatePlan((LogicalOrder &)op);
	case LogicalOperatorType::TOP_N:
		return CreatePlan((LogicalTopN &)op);
	case LogicalOperatorType::COPY_FROM_FILE:
		return CreatePlan((LogicalCopyFromFile &)op);
	case LogicalOperatorType::COPY_TO_FILE:
		return CreatePlan((LogicalCopyToFile &)op);
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
	case LogicalOperatorType::EXPRESSION_GET:
		return CreatePlan((LogicalExpressionGet &)op);
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
	case LogicalOperatorType::PREPARE:
		return CreatePlan((LogicalPrepare &)op);
	case LogicalOperatorType::EXECUTE:
		return CreatePlan((LogicalExecute &)op);
	case LogicalOperatorType::INDEX_SCAN:
		return CreatePlan((LogicalIndexScan &)op);
	case LogicalOperatorType::CREATE_VIEW:
	case LogicalOperatorType::CREATE_SEQUENCE:
	case LogicalOperatorType::CREATE_SCHEMA:
		return CreatePlan((LogicalCreate &)op);
	case LogicalOperatorType::TRANSACTION:
	case LogicalOperatorType::ALTER:
	case LogicalOperatorType::DROP:
	case LogicalOperatorType::PRAGMA:
	case LogicalOperatorType::VACUUM:
		return CreatePlan((LogicalSimple &)op);
	case LogicalOperatorType::RECURSIVE_CTE:
		return CreatePlan((LogicalRecursiveCTE &)op);
	case LogicalOperatorType::CTE_REF:
		return CreatePlan((LogicalCTERef &)op);
	default:
		throw NotImplementedException("Unimplemented logical operator type!");
	}
}
