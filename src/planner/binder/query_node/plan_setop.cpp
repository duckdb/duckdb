#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

// Optionally push a PROJECTION operator
unique_ptr<LogicalOperator> Binder::CastLogicalOperatorToTypes(vector<LogicalType> &source_types,
                                                               vector<LogicalType> &target_types,
                                                               unique_ptr<LogicalOperator> op) {
	D_ASSERT(op);
	// first check if we even need to cast
	D_ASSERT(source_types.size() == target_types.size());
	if (source_types == target_types) {
		// source and target types are equal: don't need to cast
		return op;
	}
	// otherwise add casts
	auto node = op.get();
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// "node" is a projection; we can just do the casts in there
		D_ASSERT(node->expressions.size() == source_types.size());
		if (node->children.size() == 1 && node->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
			// Is this a CSV Scan?
			auto &logical_get = node->children[0]->Cast<LogicalGet>();
			// yuck string matching, maybe there is a better way?
			if (logical_get.function.name == "read_csv" || logical_get.function.name == "read_csv_auto") {
				// We loop in the expressions and bail out if there is anything weird (i.e., not a bound column ref)
				auto &csv_bind = logical_get.bind_data->Cast<ReadCSVData>();
				// we have to do some type switcharoo
				vector<LogicalType> pushdown_types = csv_bind.csv_types;
				bool can_pushdown_types = true;
				for (idx_t i = 0; i <  op->expressions.size(); i ++) {
					if(op->expressions[i]->type == ExpressionType::BOUND_COLUMN_REF){
						auto &col_ref = op->expressions[i]->Cast<BoundColumnRefExpression>();
						pushdown_types[logical_get.column_ids[col_ref.binding.column_index]] = target_types[i];
					} else {
						can_pushdown_types = false;
						break;
					}
				}
				if (can_pushdown_types){
					csv_bind.csv_types = pushdown_types;
					csv_bind.return_types = pushdown_types;
					logical_get.returned_types = pushdown_types;
					// We early out here, since we don't need to add casts
					return op;
				}
			}
		}
		// add the casts to the selection list
		for (idx_t i = 0; i < target_types.size(); i++) {
			if (source_types[i] != target_types[i]) {
				// differing types, have to add a cast
				string cur_alias = node->expressions[i]->alias;
				node->expressions[i] =
				    BoundCastExpression::AddCastToType(context, std::move(node->expressions[i]), target_types[i]);
				node->expressions[i]->alias = cur_alias;
			}
		}
		return op;
	} else {
		// found a non-projection operator
		// push a new projection containing the casts

		// fetch the set of column bindings
		auto setop_columns = op->GetColumnBindings();
		D_ASSERT(setop_columns.size() == source_types.size());

		// now generate the expression list
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < target_types.size(); i++) {
			unique_ptr<Expression> result = make_uniq<BoundColumnRefExpression>(source_types[i], setop_columns[i]);
			if (source_types[i] != target_types[i]) {
				// add a cast only if the source and target types are not equivalent
				result = BoundCastExpression::AddCastToType(context, std::move(result), target_types[i]);
			}
			select_list.push_back(std::move(result));
		}
		auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
		projection->children.push_back(std::move(op));
		return std::move(projection);
	}
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSetOperationNode &node) {
	// Generate the logical plan for the left and right sides of the set operation
	node.left_binder->is_outside_flattened = is_outside_flattened;
	node.right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = node.left_binder->CreatePlan(*node.left);
	auto right_node = node.right_binder->CreatePlan(*node.right);

	// Add a new projection to child node
	D_ASSERT(node.left_reorder_exprs.size() == node.right_reorder_exprs.size());
	if (!node.left_reorder_exprs.empty()) {
		D_ASSERT(node.setop_type == SetOperationType::UNION_BY_NAME);
		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		// We are going to add a new projection operator, so collect the type
		// of reorder exprs in order to call CastLogicalOperatorToTypes()
		for (idx_t i = 0; i < node.left_reorder_exprs.size(); ++i) {
			left_types.push_back(node.left_reorder_exprs[i]->return_type);
			right_types.push_back(node.right_reorder_exprs[i]->return_type);
		}

		auto left_projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(node.left_reorder_exprs));
		left_projection->children.push_back(std::move(left_node));
		left_node = std::move(left_projection);

		auto right_projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(node.right_reorder_exprs));
		right_projection->children.push_back(std::move(right_node));
		right_node = std::move(right_projection);

		left_node = CastLogicalOperatorToTypes(left_types, node.types, std::move(left_node));
		right_node = CastLogicalOperatorToTypes(right_types, node.types, std::move(right_node));
	} else {
		left_node = CastLogicalOperatorToTypes(node.left->types, node.types, std::move(left_node));
		right_node = CastLogicalOperatorToTypes(node.right->types, node.types, std::move(right_node));
	}

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins || node.left_binder->has_unplanned_dependent_joins ||
	                                node.right_binder->has_unplanned_dependent_joins;

	// create actual logical ops for setops
	LogicalOperatorType logical_type;
	switch (node.setop_type) {
	case SetOperationType::UNION:
	case SetOperationType::UNION_BY_NAME:
		logical_type = LogicalOperatorType::LOGICAL_UNION;
		break;
	case SetOperationType::EXCEPT:
		logical_type = LogicalOperatorType::LOGICAL_EXCEPT;
		break;
	case SetOperationType::INTERSECT:
		logical_type = LogicalOperatorType::LOGICAL_INTERSECT;
		break;
	default:
		D_ASSERT(false);
		break;
	}

	auto root = make_uniq<LogicalSetOperation>(node.setop_index, node.types.size(), std::move(left_node),
	                                           std::move(right_node), logical_type, node.setop_all);

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb
