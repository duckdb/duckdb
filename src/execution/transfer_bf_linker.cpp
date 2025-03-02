#include "duckdb/execution/transfer_bf_linker.hpp"

namespace duckdb {

void TransferBFLinker::LinkBloomFilters(LogicalOperator &op) {
	state = State::COLLECT_BF_CREATORS;
	VisitOperator(op);

	state = State::LINK_BF_USERS;
	VisitOperator(op);
}

void TransferBFLinker::VisitOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_CREATE_BF: {
		VisitOperatorChildren(op);
		if (state == State::COLLECT_BF_CREATORS) {
			auto &create_bf_op = op.Cast<LogicalCreateBF>();
			for (auto &filter_plan : create_bf_op.bf_to_create_plans) {
				filter_creators[*filter_plan] = &create_bf_op;
			}
		}
		return;
	}
	case LogicalOperatorType::LOGICAL_USE_BF: {
		VisitOperatorChildren(op);
		if (state == State::LINK_BF_USERS) {
			auto &use_bf_op = op.Cast<LogicalUseBF>();
			for (auto &filter_plan : use_bf_op.bf_to_use_plans) {
				auto *related_creator = filter_creators[*filter_plan];
				D_ASSERT(related_creator != nullptr);
				use_bf_op.related_create_bfs.push_back(related_creator);
			}
		}
		return;
	}
	default:
		break;
	}

	VisitOperatorChildren(op);
}
} // namespace duckdb
