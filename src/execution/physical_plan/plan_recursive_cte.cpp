#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
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
	auto working_table = make_shared_ptr<ColumnDataCollection>(context, op.internal_types);

	// Add the ColumnDataCollection to the context of this PhysicalPlanGenerator
	recursive_cte_tables[op.table_index] = working_table;

	auto &left = CreatePlan(*op.children[0]);

	// If the logical operator has no key targets, then we create a normal recursive CTE operator.
	if (op.key_targets.empty()) {
		auto &right = CreatePlan(*op.children[1]);
		auto &cte = Make<PhysicalRecursiveCTE>(op.ctename, op.table_index, op.types, op.union_all, left, right,
		                                       op.estimated_cardinality);
		auto &cast_cte = cte.Cast<PhysicalRecursiveCTE>();
		cast_cte.distinct_types = op.types;
		cast_cte.working_table = working_table;
		return cte;
	}

	vector<LogicalType> distinct_types, payload_types;
	vector<idx_t> distinct_idx, payload_idx;
	vector<unique_ptr<Expression>> payload_aggregates;

	// create a group for each target, these are the columns that should be grouped
	unordered_map<idx_t, idx_t> group_by_references;
	for (idx_t i = 0; i < op.key_targets.size(); i++) {
		auto &target = op.key_targets[i];
		D_ASSERT(target->type == ExpressionType::BOUND_REF);
		auto &bound_ref = target->Cast<BoundReferenceExpression>();
		distinct_idx.emplace_back(bound_ref.index);
		distinct_types.push_back(bound_ref.return_type);
	}

	// This is used to identify which columns are involved in aggregate computations.
	unordered_map<idx_t, idx_t> aggregate_references;
	// Create a mapping of column indices to their corresponding payload aggregate indices.
	for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
		// Ensure that the payload aggregate is of the expected type.
		D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
		auto &agg = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();

		// add the logical type of the aggregate to the payload types
		payload_types.push_back(agg.return_type);
		payload_aggregates.push_back(std::move(op.payload_aggregates[i]));

		// BTODO: change payload idx determination
		switch (agg.children[0]->type) {
			case ExpressionType::BOUND_REF: {
				// Cast the child to a BoundReferenceExpression and map its index to the aggregate index.
				auto &bound_ref = agg.children[0]->Cast<BoundReferenceExpression>();
				payload_idx.emplace_back(bound_ref.index);
			    break;
		    }
			case ExpressionType::OPERATOR_CAST: {
			    auto &bound_cast = agg.children[0]->Cast<BoundCastExpression>();
			    auto ref_child = bound_cast.child.get();
			    if (ref_child->type != ExpressionType::BOUND_REF) {
				    throw BinderException("Payload aggregate must be a column reference");
			    } else {
				    auto& bound_ref = ref_child->Cast<BoundReferenceExpression>();
				    // add the logical type of the aggregate to the payload types
				    payload_idx.emplace_back(bound_ref.index);
			    }
		    	break;
		    }
			default:
			    if (agg.HasAlias()) {
				    for (idx_t idx = 0;  idx < op.names.size(); idx++) {
					    if (op.names[idx] == agg.GetAlias()) {
						    payload_idx.emplace_back(idx);
						    break;
					    }
				    }
			    } else {
				    throw BinderException("Payload aggregate must be a column reference");
			    }
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
	cast_cte.internal_types = op.internal_types;
	cast_cte.ref_recurring = op.ref_recurring;
	cast_cte.working_table = working_table;
	cast_cte.recurring_table = recurring_table;
	return cte;
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCTERef &op) {
	D_ASSERT(op.children.empty());

	// Check if this LogicalCTERef is supposed to scan a materialized CTE.
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
