#include "duckdb/execution/operator/helper/physical_unified_string_dictionary_insertion.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_unified_string_dictionary_insertion.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalUnifiedStringDictionaryInsertion &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);

	auto &usd_insertion = Make<PhysicalUnifiedStringDictionary>(op.types, std::move(op.insert_to_usd),
	                                                            op.insert_flat_vectors, op.estimated_cardinality);
	usd_insertion.children.push_back(plan);

	return usd_insertion;
}

} // namespace duckdb
