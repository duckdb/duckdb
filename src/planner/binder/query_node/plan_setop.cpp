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
unique_ptr<LogicalOperator> Binder::CastLogicalOperatorToTypes(const vector<LogicalType> &source_types,
                                                               const vector<LogicalType> &target_types,
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
			// If this projection only has one child and that child is a logical get we can try to pushdown types
			auto &logical_get = node->children[0]->Cast<LogicalGet>();
			auto &column_ids = logical_get.GetColumnIds();
			if (logical_get.function.type_pushdown) {
				unordered_map<idx_t, LogicalType> new_column_types;
				bool do_pushdown = true;
				for (idx_t i = 0; i < op->expressions.size(); i++) {
					if (op->expressions[i]->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
						auto &col_ref = op->expressions[i]->Cast<BoundColumnRefExpression>();
						auto column_id = column_ids[col_ref.binding.column_index].GetPrimaryIndex();
						if (new_column_types.find(column_id) != new_column_types.end()) {
							// Only one reference per column is accepted
							do_pushdown = false;
							break;
						}
						new_column_types[column_id] = target_types[i];
					} else {
						do_pushdown = false;
						break;
					}
				}
				if (do_pushdown) {
					logical_get.function.type_pushdown(context, logical_get.bind_data, new_column_types);
					// We also have to modify the types to the logical_get.returned_types
					for (auto &type : new_column_types) {
						logical_get.returned_types[type.first] = type.second;
					}
					return std::move(op->children[0]);
				}
			}
		}
		// add the casts to the selection list
		for (idx_t i = 0; i < target_types.size(); i++) {
			if (source_types[i] != target_types[i]) {
				// differing types, have to add a cast
				string cur_alias = node->expressions[i]->GetAlias();
				node->expressions[i] =
				    BoundCastExpression::AddCastToType(context, std::move(node->expressions[i]), target_types[i]);
				node->expressions[i]->SetAlias(cur_alias);
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
	// create actual logical ops for setops
	LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_INVALID;
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
		throw InternalException("Unsupported logical operator type for set-operation");
	}
	// Generate the logical plan for the children of the set operation

	D_ASSERT(node.bound_children.size() >= 2);
	vector<unique_ptr<LogicalOperator>> children;
	for (idx_t child_idx = 0; child_idx < node.bound_children.size(); child_idx++) {
		auto &child = node.bound_children[child_idx];
		auto &child_binder = *node.child_binders[child_idx];

		// construct the logical plan for the child node
		auto child_node = std::move(child.plan);
		// push casts for the target types
		child_node = CastLogicalOperatorToTypes(child.types, node.types, std::move(child_node));
		// check if there are any unplanned subqueries left in any child
		if (child_binder.has_unplanned_dependent_joins) {
			has_unplanned_dependent_joins = true;
		}
		children.push_back(std::move(child_node));
	}
	auto root = make_uniq<LogicalSetOperation>(node.setop_index, node.types.size(), std::move(children), logical_type,
	                                           node.setop_all);
	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb
