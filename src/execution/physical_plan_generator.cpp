#include "duckdb/execution/physical_plan_generator.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/opaque_remote_logic_get.hpp"

namespace duckdb {

class DependencyExtractor : public LogicalOperatorVisitor {
public:
	explicit DependencyExtractor(unordered_set<CatalogEntry *> &dependencies) : dependencies(dependencies) {
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

PhysicalPlanGenerator::PhysicalPlanGenerator(ClientContext &context) : context(context) {
}

PhysicalPlanGenerator::~PhysicalPlanGenerator() {
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> op) {
	auto &profiler = QueryProfiler::Get(context);

	// first resolve column references
	profiler.StartPhase("column_binding");
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);
	profiler.EndPhase();

	// now resolve types of all the operators
	profiler.StartPhase("resolve_types");
	op->ResolveOperatorTypes();
	profiler.EndPhase();

	// extract dependencies from the logical plan
	DependencyExtractor extractor(dependencies);
	extractor.VisitOperator(*op);

	// then create the main physical plan
	profiler.StartPhase("create_plan");
	auto plan = CreatePlan(*op);
	profiler.EndPhase();

	plan->Verify();
	return plan;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &op) {
	op.estimated_cardinality = op.EstimateCardinality(context);
	unique_ptr<PhysicalOperator> plan = nullptr;

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		plan = CreatePlan((LogicalGet &)op);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		plan = CreatePlan((LogicalProjection &)op);
		break;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		plan = CreatePlan((LogicalEmptyResult &)op);
		break;
	case LogicalOperatorType::LOGICAL_FILTER:
		plan = CreatePlan((LogicalFilter &)op);
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		plan = CreatePlan((LogicalAggregate &)op);
		break;
	case LogicalOperatorType::LOGICAL_WINDOW:
		plan = CreatePlan((LogicalWindow &)op);
		break;
	case LogicalOperatorType::LOGICAL_UNNEST:
		plan = CreatePlan((LogicalUnnest &)op);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT:
		plan = CreatePlan((LogicalLimit &)op);
		break;
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		plan = CreatePlan((LogicalLimitPercent &)op);
		break;
	case LogicalOperatorType::LOGICAL_SAMPLE:
		plan = CreatePlan((LogicalSample &)op);
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		plan = CreatePlan((LogicalOrder &)op);
		break;
	case LogicalOperatorType::LOGICAL_TOP_N:
		plan = CreatePlan((LogicalTopN &)op);
		break;
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		plan = CreatePlan((LogicalCopyToFile &)op);
		break;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		plan = CreatePlan((LogicalDummyScan &)op);
		break;
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		plan = CreatePlan((LogicalAnyJoin &)op);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		plan = CreatePlan((LogicalDelimJoin &)op);
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		plan = CreatePlan((LogicalComparisonJoin &)op);
		break;
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		plan = CreatePlan((LogicalCrossProduct &)op);
		break;
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
		plan = CreatePlan((LogicalSetOperation &)op);
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		plan = CreatePlan((LogicalInsert &)op);
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		plan = CreatePlan((LogicalDelete &)op);
		break;
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		plan = CreatePlan((LogicalColumnDataGet &)op);
		break;
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		plan = CreatePlan((LogicalDelimGet &)op);
		break;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		plan = CreatePlan((LogicalExpressionGet &)op);
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		plan = CreatePlan((LogicalUpdate &)op);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		plan = CreatePlan((LogicalCreateTable &)op);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		plan = CreatePlan((LogicalCreateIndex &)op);
		break;
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		plan = CreatePlan((LogicalExplain &)op);
		break;
	case LogicalOperatorType::LOGICAL_SHOW:
		plan = CreatePlan((LogicalShow &)op);
		break;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		plan = CreatePlan((LogicalDistinct &)op);
		break;
	case LogicalOperatorType::LOGICAL_PREPARE:
		plan = CreatePlan((LogicalPrepare &)op);
		break;
	case LogicalOperatorType::LOGICAL_EXECUTE:
		plan = CreatePlan((LogicalExecute &)op);
		break;
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		plan = CreatePlan((LogicalCreate &)op);
		break;
	case LogicalOperatorType::LOGICAL_PRAGMA:
		plan = CreatePlan((LogicalPragma &)op);
		break;
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_VACUUM:
	case LogicalOperatorType::LOGICAL_LOAD:
		plan = CreatePlan((LogicalSimple &)op);
		break;
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		plan = CreatePlan((LogicalRecursiveCTE &)op);
		break;
	case LogicalOperatorType::LOGICAL_CTE_REF:
		plan = CreatePlan((LogicalCTERef &)op);
		break;
	case LogicalOperatorType::LOGICAL_EXPORT:
		plan = CreatePlan((LogicalExport &)op);
		break;
	case LogicalOperatorType::LOGICAL_SET:
		plan = CreatePlan((LogicalSet &)op);
		break;
	case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
		auto &extension_op = (LogicalExtensionOperator &)op;
		plan = extension_op.CreatePlan(context, *this);

		if (!plan)
			throw InternalException("Missing PhysicalOperator for Extension Operator");
		break;
	default: {
		throw NotImplementedException("Unimplemented logical operator type!");
	}
	}

	if (op.estimated_props) {
		plan->estimated_cardinality = op.estimated_props->GetCardinality<idx_t>();
		plan->estimated_props = op.estimated_props->Copy();
	} else {
		plan->estimated_props = make_unique<EstimatedProperties>();
	}

	return plan;
}

} // namespace duckdb
