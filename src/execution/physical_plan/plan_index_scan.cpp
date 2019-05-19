#include "execution/operator/filter/physical_filter.hpp"
#include "execution/operator/scan/physical_index_scan.hpp"
#include "execution/physical_plan_generator.hpp"
#include "optimizer/matcher/expression_matcher.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "planner/expression/bound_comparison_expression.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/operator/logical_filter.hpp"
#include "planner/operator/logical_get.hpp"
#include "storage/data_table.hpp"

#include <planner/operator/list.hpp>

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalIndexScan &op) {
	unique_ptr<PhysicalOperator> plan;
	auto node = make_unique<PhysicalIndexScan>(op, op.tableref, op.table, op.index, op.column_ids);
	if (op.equal_index) {
		node->equal_value = op.equal_value;
		node->equal_index = true;
	}
	if (op.low_index) {
		node->low_value = op.low_value;
		node->low_index = true;
		node->low_expression_type = op.low_expression_type;
	}
	if (op.high_index) {
		node->high_value = op.high_value;
		node->high_index = true;
		node->high_expression_type = op.high_expression_type;
	}
	plan = move(node);
	return plan;
}
