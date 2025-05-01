#include "duckdb/execution/transfer_bf_linker.hpp"

#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

void TransferBFLinker::LinkBFOperators(LogicalOperator &op) {
	state = State::COLLECT_BF_CREATORS;
	VisitOperator(op);

	state = State::LINK_BF_USERS;
	VisitOperator(op);

	state = State::CLEAN_USELESS_OPERATORS;
	VisitOperator(op);

	state = State::UPDATE_MIN_MAX_BINDING;
	VisitOperator(op);
}

void TransferBFLinker::VisitOperator(LogicalOperator &op) {
	switch (state) {
	case State::COLLECT_BF_CREATORS: {
		if (op.type == LogicalOperatorType::LOGICAL_CREATE_BF) {
			auto &create_bf_op = op.Cast<LogicalCreateBF>();

			// Compact consecutive creators: creator --> creator --> scan ==> new_creator --> scan
			while (create_bf_op.children[0]->type == LogicalOperatorType::LOGICAL_CREATE_BF) {
				auto &child_creator = create_bf_op.children[0]->Cast<LogicalCreateBF>();
				for (auto &filter_plan : child_creator.filter_plans) {
					create_bf_op.filter_plans.push_back(filter_plan);
				}
				create_bf_op.children[0] = std::move(child_creator.children[0]);
			}

			for (auto &filter_plan : create_bf_op.filter_plans) {
				bf_creators[filter_plan.get()] = &create_bf_op;
			}
			create_bf_op.min_max_to_create.resize(create_bf_op.filter_plans.size());
			create_bf_op.min_max_applied_cols.resize(create_bf_op.filter_plans.size());
		}
		break;
	}
	case State::LINK_BF_USERS: {
		if (op.type == LogicalOperatorType::LOGICAL_USE_BF) {
			auto &bf_user = op.Cast<LogicalUseBF>();
			auto &filter_plan = bf_user.filter_plan;
			auto *related_creator = bf_creators[filter_plan.get()];

			if (related_creator != nullptr) {
				idx_t plan_idx = FindPlanIndex(bf_user.filter_plan, related_creator->filter_plans);
				// after this, two plans point to the same memory
				bf_user.filter_plan = related_creator->filter_plans[plan_idx];
				bf_user.related_create_bf = related_creator;
				useful_creator.insert(related_creator);
			}
		}
		break;
	}
	case State::CLEAN_USELESS_OPERATORS: {
		for (size_t i = 0; i < op.children.size(); i++) {
			auto &child = op.children[i];
			while (true) {
				if (child->type == LogicalOperatorType::LOGICAL_CREATE_BF && !useful_creator.count(child.get())) {
					child = std::move(child->children[0]);
					continue;
				}
				if (child->type == LogicalOperatorType::LOGICAL_USE_BF) {
					auto &user = child->Cast<LogicalUseBF>();
					if (user.related_create_bf == nullptr) {
						auto moved_child = std::move(child); // Avoid use-after-move
						child = make_uniq<LogicalEmptyResult>(std::move(moved_child));
						break;
					}
				}
				break;
			}
		}
		break;
	}
	case State::UPDATE_MIN_MAX_BINDING: {
		if (op.type == LogicalOperatorType::LOGICAL_USE_BF) {
			auto &bf_user = op.Cast<LogicalUseBF>();

			bool all_numerical = true;
			for (auto &expr : bf_user.filter_plan->apply) {
				auto &col_binding = expr->Cast<BoundColumnRefExpression>();
				if (!col_binding.return_type.IsNumeric()) {
					all_numerical = false;
					break;
				}
			}

			if (all_numerical) {
				vector<ColumnBinding> updated_bindings;
				shared_ptr<DynamicTableFilterSet> filter_set = nullptr;
				for (auto &expr : bf_user.filter_plan->apply) {
					auto &binding = expr->Cast<BoundColumnRefExpression>().binding;
					updated_bindings.push_back(binding);
				}
				UpdateMinMaxBinding(*bf_user.children[0], updated_bindings, filter_set);

				if (filter_set) {
					auto *related_creator = bf_user.related_create_bf;
					idx_t plan_idx = FindPlanIndex(bf_user.filter_plan, related_creator->filter_plans);
					related_creator->min_max_to_create[plan_idx] = filter_set;
					related_creator->min_max_applied_cols[plan_idx] = std::move(updated_bindings);
				}
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			if (join.join_type == JoinType::MARK) {
				// min-max filter does not support mark join
				return;
			}
		}
		break;
	}
	default:
		break;
	}

	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

idx_t TransferBFLinker::FindPlanIndex(const shared_ptr<FilterPlan> &plan,
                                      const vector<shared_ptr<FilterPlan>> &filter_plans) {
	idx_t plan_idx = std::numeric_limits<idx_t>::max();
	for (idx_t i = 0; i < filter_plans.size(); i++) {
		auto &creator_plan = filter_plans[i];
		if (*creator_plan == *plan) {
			plan_idx = i;
			break;
		}
	}
	return plan_idx;
}

void TransferBFLinker::UpdateMinMaxBinding(LogicalOperator &op, vector<ColumnBinding> &updated_bindings,
                                           shared_ptr<DynamicTableFilterSet> &filter_set) {
	auto &child = op;
	switch (child.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = child.Cast<LogicalGet>();
		if (!get.function.filter_pushdown) {
			// filter pushdown is not supported - no need to consider this node
			return;
		}

		if (!get.dynamic_filters) {
			get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
		}

		filter_set = get.dynamic_filters;
		return;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection - check if we all of the expressions are only column references
		auto &proj = child.Cast<LogicalProjection>();
		for (auto &binding : updated_bindings) {
			auto &expr = *proj.expressions[binding.column_index];
			binding = expr.Cast<BoundColumnRefExpression>().binding;
		}
		UpdateMinMaxBinding(*child.children[0], updated_bindings, filter_set);
		break;
	}
	case LogicalOperatorType::LOGICAL_USE_BF:
	case LogicalOperatorType::LOGICAL_CREATE_BF:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// does not affect probe side - recurse into left child
		UpdateMinMaxBinding(*child.children[0], updated_bindings, filter_set);
		break;
	}
	default:
		break;
	}
}
} // namespace duckdb
