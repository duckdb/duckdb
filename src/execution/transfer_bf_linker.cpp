#include "duckdb/execution/transfer_bf_linker.hpp"

#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
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

	state = State::SMOOTH_MARK_JOIN;
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
		}
		break;
	}
	case State::SMOOTH_MARK_JOIN: {
		// Reorder BF-related operators above a MARK join:
		// From: op -> filter -> join(MARK) -> create_bf_chain -> real_child
		// To:   op -> create_bf_chain -> filter -> join(MARK) -> real_child
		if (op.children.empty() || op.children[0]->type != LogicalOperatorType::LOGICAL_FILTER) {
			break;
		}

		auto &filter = op.children[0]->Cast<LogicalFilter>();
		if (filter.expressions.size() != 1 || filter.expressions[0]->type != ExpressionType::BOUND_COLUMN_REF ||
		    filter.children.empty() || filter.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			break;
		}

		auto &join = filter.children[0]->Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::MARK || !(join.children[0]->type == LogicalOperatorType::LOGICAL_CREATE_BF ||
		                                          join.children[0]->type == LogicalOperatorType::LOGICAL_USE_BF)) {
			break;
		}

		// Step 1: Identify the last CREATE_BF in the BF chain (which may include USE_BF in between)
		LogicalOperator *bf_tail = join.children[0].get();
		LogicalOperator *last_creator = nullptr;

		while (bf_tail->type == LogicalOperatorType::LOGICAL_CREATE_BF ||
		       bf_tail->type == LogicalOperatorType::LOGICAL_USE_BF) {
			if (bf_tail->type == LogicalOperatorType::LOGICAL_CREATE_BF) {
				last_creator = bf_tail;
			}
			bf_tail = bf_tail->children[0].get();
		}

		// If there is no CREATE_BF in the chain, skip the transformation
		if (!last_creator) {
			break;
		}

		bf_tail = last_creator;
		D_ASSERT(bf_tail->type == LogicalOperatorType::LOGICAL_CREATE_BF);

		// Step 2: Detach the BF chain from the join and isolate the real child that follows it
		auto bf_chain = std::move(join.children[0]);
		auto real_child = std::move(bf_tail->children[0]);

		// Step 3: Rewire the filter subtree under the end of the BF chain
		bf_tail->children[0] = std::move(op.children[0]);

		// Step 4: Fix the join's left child to point to the real child
		auto &new_filter = bf_tail->children[0]->Cast<LogicalFilter>();
		auto &new_join = new_filter.children[0]->Cast<LogicalComparisonJoin>();
		new_join.children[0] = std::move(real_child);

		// Step 5: Replace op's child with the head of the new reordered BF chain
		op.children[0] = std::move(bf_chain);
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
