#include "duckdb/execution/transfer_bf_linker.hpp"

#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_use_bf.hpp"

namespace duckdb {

void TransferBFLinker::RemoveUselessOperators(LogicalOperator &op) {
	state = State::COLLECT_BF_CREATORS;
	VisitOperator(op);

	state = State::LINK_BF_USERS;
	VisitOperator(op);

	state = State::CLEAN_USELESS_OPERATORS;
	VisitOperator(op);
}

void TransferBFLinker::VisitOperator(LogicalOperator &op) {
	switch (state) {
	case State::COLLECT_BF_CREATORS: {
		if (op.type == LogicalOperatorType::LOGICAL_CREATE_BF) {
			auto &create_bf_op = op.Cast<LogicalCreateBF>();
			for (auto &filter_plan : create_bf_op.bf_to_create_plans) {
				bf_creators[*filter_plan] = &create_bf_op;
			}
		}
		break;
	}
	case State::LINK_BF_USERS: {
		if (op.type == LogicalOperatorType::LOGICAL_USE_BF) {
			auto &use_bf_op = op.Cast<LogicalUseBF>();
			auto &filter_plan = use_bf_op.bf_to_use_plan;
			auto *related_creator = bf_creators[*filter_plan];

			if (related_creator != nullptr) {
				use_bf_op.related_create_bf = related_creator;
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
	}

	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

} // namespace duckdb
