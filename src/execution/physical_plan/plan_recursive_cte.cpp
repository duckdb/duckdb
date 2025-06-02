#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/perfect_aggregate_hashtable.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalRecursiveCTE &op) {
	D_ASSERT(op.children.size() == 2);

	// Create the working_table that the PhysicalRecursiveCTE will use for evaluation.
	auto working_table = make_shared_ptr<ColumnDataCollection>(context, op.types);

	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;

	auto &left = CreatePlan(*op.children[0]);

	// If the logical operator has no key targets or all columns are referenced,
	// then we create a normal recursive CTE operator.
	if (op.key_targets.empty()) {
		auto &right = CreatePlan(*op.children[1]);
		auto &cte = Make<PhysicalRecursiveCTE>(op.ctename, op.table_index, op.types, op.union_all, left, right,
		                                       op.estimated_cardinality);
		auto &cast_cte = cte.Cast<PhysicalRecursiveCTE>();
		cast_cte.distinct_types = op.types;
		cast_cte.working_table = working_table;
		return cte;
	}

	vector<LogicalType> payload_types, distinct_types;
	vector<idx_t> payload_idx, distinct_idx;
	vector<unique_ptr<BoundAggregateExpression>> payload_aggregates;

	// create a group for each target, these are the columns that should be grouped
	unordered_map<idx_t, idx_t> group_by_references;
	for (idx_t i = 0; i < op.key_targets.size(); i++) {
		auto &target = op.key_targets[i];
		D_ASSERT(target->type == ExpressionType::BOUND_REF);
		auto &bound_ref = target->Cast<BoundReferenceExpression>();
		group_by_references[bound_ref.index] = i;
	}

	/*
	 * iterate over all types
	 * 		Differentiate the occurrence of the column in the key clause.
	 */
	auto &types = left.GetTypes();
	for (idx_t i = 0; i < types.size(); ++i) {
		auto logical_type = types[i];
		// Check if we can directly refer to a group, or if we need to push an aggregate with LAST
		auto entry = group_by_references.find(i);
		if (entry != group_by_references.end()) {
			// Column has a key, note the column index to make a distinction on it
			distinct_idx.emplace_back(i);
			distinct_types.push_back(logical_type);
		} else {
			// Column is not in the key clause, so we need to create an aggregate
			auto bound = make_uniq<BoundReferenceExpression>(logical_type, 0U);

			vector<unique_ptr<Expression>> first_children;
			first_children.push_back(std::move(bound));

			FunctionBinder function_binder(context);
			auto first_aggregate =
			    function_binder.BindAggregateFunction(LastFunctionGetter::GetFunction(logical_type),
			                                          std::move(first_children), nullptr, AggregateType::NON_DISTINCT);
			first_aggregate->order_bys = nullptr;

			payload_types.push_back(logical_type);
			payload_idx.emplace_back(i);
			payload_aggregates.push_back(std::move(first_aggregate));
		}
	}

	// If the key variant has been used, a recurring table will be created.
	auto recurring_table = make_shared_ptr<ColumnDataCollection>(context, op.types);
	recurring_cte_tables[op.table_index] = recurring_table;

	auto &right = CreatePlan(*op.children[1]);
	auto &cte = Make<PhysicalRecursiveCTE>(op.ctename, op.table_index, op.types, op.union_all, left, right,
	                                       op.estimated_cardinality);
	auto &cast_cte = cte.Cast<PhysicalRecursiveCTE>();
	cast_cte.using_key = true;
	cast_cte.payload_aggregates = std::move(payload_aggregates);
	cast_cte.distinct_idx = distinct_idx;
	cast_cte.distinct_types = distinct_types;
	cast_cte.payload_idx = payload_idx;
	cast_cte.payload_types = payload_types;
	cast_cte.ref_recurring = op.ref_recurring;
	cast_cte.working_table = working_table;
	cast_cte.recurring_table = recurring_table;
	return cte;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
	D_ASSERT(op.children.empty());

	// Check if this LogicalCTERef is supposed to scan a materialized CTE.
	if (op.materialized_cte == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
		// Lookup if there is a materialized CTE for the cte_index.
		auto materialized_cte = materialized_ctes.find(op.cte_index);

		// If this check fails, this is a reference to a materialized recursive CTE.
		if (materialized_cte != materialized_ctes.end()) {
			auto &chunk_scan = Make<PhysicalColumnDataScan>(op.chunk_types, PhysicalOperatorType::CTE_SCAN,
			                                                op.estimated_cardinality, op.cte_index);

			auto cte = recursive_cte_tables.find(op.cte_index);
			if (cte == recursive_cte_tables.end()) {
				throw InvalidInputException("Referenced materialized CTE does not exist.");
			}

			auto &cast_chunk_scan = chunk_scan.Cast<PhysicalColumnDataScan>();
			cast_chunk_scan.collection = cte->second.get();
			materialized_cte->second.push_back(cast_chunk_scan);
			return chunk_scan;
		}
	}

	// CreatePlan of a LogicalRecursiveCTE must have happened before.
	auto cte = recursive_cte_tables.find(op.cte_index);
	if (cte == recursive_cte_tables.end()) {
		throw InvalidInputException("Referenced recursive CTE does not exist.");
	}

	// If we found a recursive CTE and we want to scan the recurring table, we search for it,
	if (op.is_recurring) {
		cte = recurring_cte_tables.find(op.cte_index);
		if (cte == recurring_cte_tables.end()) {
			throw InvalidInputException("RECURRING can only be used with USING KEY in recursive CTE.");
		}
	}

	auto &types = cte->second.get()->Types();
	auto op_type =
	    op.is_recurring ? PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN : PhysicalOperatorType::RECURSIVE_CTE_SCAN;
	auto &chunk_scan = Make<PhysicalColumnDataScan>(types, op_type, op.estimated_cardinality, op.cte_index);
	auto &cast_chunk_scan = chunk_scan.Cast<PhysicalColumnDataScan>();
	cast_chunk_scan.collection = cte->second.get();
	return chunk_scan;
}

} // namespace duckdb
