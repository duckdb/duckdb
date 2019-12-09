#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPrepare &op) {
	assert(op.children.size() == 1);

	// generate physical plan
	auto plan = CreatePlan(*op.children[0]);
	op.prepared->types = plan->types;
	op.prepared->plan = move(plan);

	// finally create the catalog entry
	auto entry = make_unique<PreparedStatementCatalogEntry>(op.name, move(op.prepared));
	entry->catalog = &context.catalog;

	// now store plan in context
	if (!context.prepared_statements->CreateEntry(context.ActiveTransaction(), op.name, move(entry), dependencies)) {
		throw Exception("Failed to prepare statement");
	}
	vector<TypeId> prep_return_types = {TypeId::BOOLEAN};
	return make_unique<PhysicalDummyScan>(prep_return_types);
}
