#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_prepare.hpp"
#include "execution/operator/scan/physical_dummy_scan.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

// // we use this to find parameters in the prepared statement
// class PrepareParameterVisitor : public SQLNodeVisitor {
// public:
// 	PrepareParameterVisitor(unordered_map<size_t, ParameterExpression *> &parameter_expression_map)
// 	    : parameter_expression_map(parameter_expression_map) {
// 	}

// protected:
// 	void Visit(ParameterExpression &expr) override {
// 		if (expr.return_type == TypeId::INVALID) {
// 			throw Exception("Could not determine type for prepared statement parameter. Consider using a CAST on it.");
// 		}
// 		auto it = parameter_expression_map.find(expr.parameter_nr);
// 		if (it != parameter_expression_map.end()) {
// 			throw Exception("Duplicate parameter index. Use $1, $2 etc. to differentiate.");
// 		}
// 		parameter_expression_map[expr.parameter_nr] = &expr;
// 	}

// private:
// 	unordered_map<size_t, ParameterExpression *> &parameter_expression_map;
// };

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPrepare &op) {
	assert(op.children.size() == 1);

	// create the physical plan for the prepare statement.
	auto entry = make_unique<PreparedStatementCatalogEntry>(op.name, op.statement_type);
	entry->names = op.names;

	// find tables
	op.GetTableBindings(entry->tables);

	// generate physical plan
	auto plan = CreatePlan(*op.children[0]);

	// // find parameters and add to info
	// PrepareParameterVisitor ppv(entry->parameter_expression_map);
	// plan->Accept(&ppv);

	entry->types = plan->types;
	entry->plan = move(plan);

	// now store plan in context
	if (!context.prepared_statements->CreateEntry(context.ActiveTransaction(), op.name, move(entry))) {
		throw Exception("Failed to prepare statement");
	}
	vector<TypeId> prep_return_types = {TypeId::BOOLEAN};
	return make_unique<PhysicalDummyScan>(prep_return_types);
}
