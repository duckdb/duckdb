#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	// now visit the children
	assert(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	assert(left && right);

	if (op.conditions.size() == 0) {
		// no conditions: insert a cross product
		return make_unique<PhysicalCrossProduct>(op.types, move(left), move(right));
	}

	bool has_equality = false;
	bool has_inequality = false;
#ifndef NDEBUG
	bool has_null_equal_conditions = false;
#endif
	for (auto &cond : op.conditions) {
		if (cond.comparison == ExpressionType::COMPARE_EQUAL) {
			has_equality = true;
		}
		if (cond.comparison == ExpressionType::COMPARE_NOTEQUAL) {
			has_inequality = true;
		}
		if (cond.null_values_are_equal) {
#ifndef NDEBUG
			has_null_equal_conditions = true;
#endif
			assert(cond.comparison == ExpressionType::COMPARE_EQUAL);
		}
	}
	unique_ptr<PhysicalOperator> plan;
	if (has_equality) {
		// check if one of the tables has an index on column
		if (op.join_type == JoinType::INNER && op.conditions.size() == 1) {
			// check if one of the children are table scans and if they have an index in the join attribute
			// (op.condition)
			Index *left_index{}, *right_index{};
			if (left->type == PhysicalOperatorType::TABLE_SCAN) {
				auto &tbl_scan = (PhysicalTableScan &)*left;
				auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
				if (tbl) {
					for (auto &index : tbl->table->storage->info->indexes) {
						if (index->unbound_expressions[0]->alias == op.conditions[0].left->alias) {
							// Hooray, this column is indexed
							left_index = index.get();
							break;
						}
					}
				}
			}
			if (right->type == PhysicalOperatorType::TABLE_SCAN) {
				auto &tbl_scan = (PhysicalTableScan &)*right;
				auto tbl = dynamic_cast<TableScanBindData *>(tbl_scan.bind_data.get());
				if (tbl) {
					for (auto &index : tbl->table->storage->info->indexes) {
						if (index->unbound_expressions[0]->alias == op.conditions[0].right->alias) {
							// Hooray, this column is indexed
							right_index = index.get();
							break;
						}
					}
				}
			}
			if (left_index) {
				swap(op.conditions[0].left, op.conditions[0].right);
				return make_unique<PhysicalIndexJoin>(op, move(right), move(left), move(op.conditions), op.join_type,
				                                      op.right_projection_map, op.left_projection_map, left_index);
				//				if (right){
				//					// Uh, index in both condition sides, which one to use?
				//				}
			}
			if (right_index) {
				return make_unique<PhysicalIndexJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
				                                      op.left_projection_map, op.right_projection_map, right_index);
			}
		}
		// equality join: use hash join
		plan = make_unique<PhysicalHashJoin>(op, move(left), move(right), move(op.conditions), op.join_type,
		                                     op.left_projection_map, op.right_projection_map);
	} else {
		assert(!has_null_equal_conditions); // don't support this for anything but hash joins for now
		if (op.conditions.size() == 1 && !has_inequality) {
			// range join: use piecewise merge join
			plan =
			    make_unique<PhysicalPiecewiseMergeJoin>(op, move(left), move(right), move(op.conditions), op.join_type);
		} else {
			// inequality join: use nested loop
			plan = make_unique<PhysicalNestedLoopJoin>(op, move(left), move(right), move(op.conditions), op.join_type);
		}
	}
	return plan;
}

} // namespace duckdb
