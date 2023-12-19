#include "duckdb/optimizer/operation_converter.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "iostream"
namespace duckdb {

OperationConverter::OperationConverter(LogicalOperator &root, Binder &binder) : root(root), binder(binder) {
	root.ResolveOperatorTypes();
}

void OperationConverter::Optimize(unique_ptr<LogicalOperator> &op, bool is_root) {
	for (auto &child : op->children) {
		Optimize(child);
	}
	switch (op->type) {
		// if it is setop all, we don't replace (even though we technically still could)
		// if it is not setop all, duplicate elimination should happen
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		auto &set_op = op->Cast<LogicalSetOperation>();
		auto setop_all = set_op.setop_all;
		if (is_root) {
			// if the operator is the root, then break. We don't want to update the root
			// and the join order optimizer can still have an effect.
			break;
		}

		auto table_index = set_op.table_index;
		auto &left = op->children[0];
		auto left_types = op->children[0]->types;
		auto &right = op->children[1];
		auto right_types = op->children[1]->types;

		auto old_bindings = op->GetColumnBindings();

		if (setop_all) {

			// instead create a logical projection on top of whatever to add the window expression, then
			auto left_window_table_index = binder.GenerateTableIndex();
			auto left_window = make_uniq<LogicalWindow>(left_window_table_index);
			auto row_number =
			    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
			row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
			row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
			auto left_bindings = left->GetColumnBindings();
			for (idx_t i = 0; i < left_types.size(); i++) {
				row_number->partitions.push_back(make_uniq<BoundColumnRefExpression>(left_types[i], left_bindings[i]));
			}
			left_window->expressions.push_back(std::move(row_number));
			left_window->AddChild(std::move(left));
			left = std::move(left_window);

			auto right_window_table_index = binder.GenerateTableIndex();
			auto right_window = make_uniq<LogicalWindow>(right_window_table_index);
			row_number =
			    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
			row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
			row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
			auto right_bindings = right->GetColumnBindings();
			for (idx_t i = 0; i < right_bindings.size(); i++) {
				row_number->partitions.push_back(make_uniq<BoundColumnRefExpression>(right_types[i], right_bindings[i]));
			}
			right_window->expressions.push_back(std::move(row_number));
			right_window->AddChild(std::move(right));
			right = std::move(right_window);

			set_op.types.push_back(LogicalType::BIGINT);
			set_op.column_count += 1;
		}
		auto left_bindings = left->GetColumnBindings();
		auto right_bindings = right->GetColumnBindings();
		D_ASSERT(left_bindings.size() == right_bindings.size());

		if (setop_all) {
			D_ASSERT(left_types.size() + 1 == left_bindings.size());
			D_ASSERT(right_types.size() + 1 == right_bindings.size());
		}
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < left_types.size(); i++) {
			JoinCondition cond;
			cond.left = make_uniq<BoundColumnRefExpression>(left_types[i], left_bindings[i]);
			cond.right = make_uniq<BoundColumnRefExpression>(right_types[i], right_bindings[i]);
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(std::move(cond));
		}

		if (setop_all) {
			JoinCondition cond;
			cond.left =
			    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, left_bindings[left_bindings.size() - 1]);
			cond.right =
			    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, right_bindings[right_bindings.size() - 1]);
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(std::move(cond));
		}

		JoinType join_type = op->type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;

		auto join_op = make_uniq<LogicalComparisonJoin>(join_type);
		join_op->children.push_back(std::move(left));
		join_op->children.push_back(std::move(right));
		join_op->conditions = std::move(conditions);
		join_op->ResolveOperatorTypes();

		op = std::move(join_op);

		// create projection to remove row_id.
		if (setop_all) {
			vector<unique_ptr<Expression>> projection_select_list;
			auto bindings = op->GetColumnBindings();
			for (idx_t i = 0; i < bindings.size() - 1; i++) {
				projection_select_list.push_back(make_uniq<BoundColumnRefExpression>(op->types[i], bindings[i]));
			}
			auto projection_table_index = binder.GenerateTableIndex();
			auto projection =
			    make_uniq<LogicalProjection>(projection_table_index, std::move(projection_select_list));
			projection->children.push_back(std::move(op));
			projection->ResolveOperatorTypes();
			op = std::move(projection);
		}

		if (!setop_all) {
			// push a distinct operator on the join
			auto &types = op->types;
			auto join_bindings = op->GetColumnBindings();
			vector<unique_ptr<Expression>> distinct_targets;
			vector<unique_ptr<Expression>> select_list;
			for (idx_t i = 0; i < join_bindings.size(); i++) {
				distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(types[i], join_bindings[i]));
				select_list.push_back(make_uniq<BoundColumnRefExpression>(types[i], join_bindings[i]));
			}
			auto distinct = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
			distinct->children.push_back(std::move(op));
			op = std::move(distinct);

			auto projection = make_uniq<LogicalProjection>(table_index, std::move(select_list));
			projection->children.push_back(std::move(op));
			op = std::move(projection);
			op->ResolveOperatorTypes();
		}
		// now perform column binding replacement
		auto new_bindings = op->GetColumnBindings();

//		D_ASSERT(old_bindings.size() == new_bindings.size());
		vector<ReplacementBinding> replacement_bindings;
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacement_bindings.push_back({old_bindings[i], new_bindings[i]});
		}

		auto binding_replacer = ColumnBindingReplacer();
		binding_replacer.replacement_bindings = replacement_bindings;
		binding_replacer.VisitOperator(root);
		break;
	}
	default:
		break;
	}
}

} // namespace duckdb
