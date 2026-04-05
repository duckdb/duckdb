#include "duckdb/execution/trigger_executor.hpp"

#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

constexpr idx_t TriggerExecutor::MAX_TRIGGER_DEPTH;

struct TriggerDepthGuard {
	explicit TriggerDepthGuard(idx_t &depth) : depth(depth) {
		depth++;
	}
	~TriggerDepthGuard() {
		depth--;
	}
	idx_t &depth;
};

// Planning requires a SQLStatement,
// but the trigger body is stored as a QueryNode (since SQLStatement is not serializable)
static unique_ptr<SQLStatement> WrapQueryNode(QueryNode &body) {
	auto body_copy = body.Copy();
	switch (body_copy->type) {
	case QueryNodeType::INSERT_QUERY_NODE: {
		auto stmt = make_uniq<InsertStatement>();
		stmt->node = unique_ptr_cast<QueryNode, InsertQueryNode>(std::move(body_copy));
		return stmt;
	}
	case QueryNodeType::UPDATE_QUERY_NODE: {
		auto stmt = make_uniq<UpdateStatement>();
		stmt->node = unique_ptr_cast<QueryNode, UpdateQueryNode>(std::move(body_copy));
		return stmt;
	}
	case QueryNodeType::DELETE_QUERY_NODE: {
		auto stmt = make_uniq<DeleteStatement>();
		stmt->node = unique_ptr_cast<QueryNode, DeleteQueryNode>(std::move(body_copy));
		return stmt;
	}
	default:
		throw InternalException("Unexpected trigger body query node type");
	}
}

static unique_ptr<PhysicalPlan> PlanTriggerBody(ClientContext &context, QueryNode &body) {
	Planner planner(context);
	planner.CreatePlan(WrapQueryNode(body));
	if (!planner.plan) {
		return nullptr;
	}

	Optimizer optimizer(*planner.binder, context);
	auto logical_plan = optimizer.Optimize(std::move(planner.plan));

	PhysicalPlanGenerator physical_generator(context);
	return physical_generator.Plan(std::move(logical_plan));
}

static void ExecuteTriggerBody(ClientContext &context, PhysicalPlan &plan) {
	Executor trigger_executor(context);
	trigger_executor.Initialize(plan.Root());
	while (!trigger_executor.ExecutionIsFinished()) {
		auto result = trigger_executor.ExecuteTask();
		if (result == PendingExecutionResult::NO_TASKS_AVAILABLE || result == PendingExecutionResult::BLOCKED) {
			trigger_executor.WaitForTask();
		}
	}
	// CancelTasks() spins until all worker threads release their tasks,
	// preventing a race with ~Executor() which asserts executor_tasks == 0.
	trigger_executor.CancelTasks();
	if (trigger_executor.HasError()) {
		trigger_executor.ThrowException();
	}
}

void TriggerExecutor::Fire(ClientContext &context, const vector<TriggerInfo> &triggers, idx_t row_count) {
	if (triggers.empty()) {
		return;
	}
	if (context.trigger_depth >= MAX_TRIGGER_DEPTH) {
		throw InvalidInputException("Trigger recursion depth limit (%llu) exceeded.", MAX_TRIGGER_DEPTH);
	}
	TriggerDepthGuard depth_guard(context.trigger_depth);

	for (auto &trigger : triggers) {
		// Plan the body once regardless of row count
		auto physical_plan = PlanTriggerBody(context, *trigger.body);
		if (!physical_plan) {
			continue;
		}
		if (trigger.for_each == TriggerForEach::ROW) {
			for (idx_t i = 0; i < row_count; i++) {
				ExecuteTriggerBody(context, *physical_plan);
			}
		} else {
			ExecuteTriggerBody(context, *physical_plan);
		}
	}
}

} // namespace duckdb
