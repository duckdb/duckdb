#include "duckdb/execution/trigger_executor.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
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

static void ExecuteTriggerBody(ClientContext &context, const string &sql_body_text) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sql_body_text);
	if (parser.statements.empty()) {
		return;
	}

	Planner planner(context);
	planner.CreatePlan(std::move(parser.statements[0]));
	if (!planner.plan) {
		return;
	}

	Optimizer optimizer(*planner.binder, context);
	auto logical_plan = optimizer.Optimize(std::move(planner.plan));

	PhysicalPlanGenerator physical_generator(context);
	auto physical_plan = physical_generator.Plan(std::move(logical_plan));

	Executor trigger_executor(context);
	trigger_executor.Initialize(physical_plan->Root());

	while (!trigger_executor.ExecutionIsFinished()) {
		auto result = trigger_executor.ExecuteTask();
		if (result == PendingExecutionResult::NO_TASKS_AVAILABLE || result == PendingExecutionResult::BLOCKED) {
			// The trigger body's tasks are being executed by worker threads since ExecutionIsFinished() is false,
			// but we have nothing to do (no tasks / blocked). We will just have to wait.
			trigger_executor.WaitForTask();
		}
	}
	// Race condition handling:
	// When we this function finishes, the `trigger_executor` will be destructed.
	// ~Executor() asserts executor_tasks == 0. executor_tasks is decremented in ~ExecutorTask(),
	// which is about to be executed when a worker thread drops its shared_ptr<Task>,
	// but this happens after ExecutionIsFinished() true and our loop exits.
	// Calling CancelTasks() makes sure to hold before the local executor destructs, as it spins on `executor_tasks > 0`
	trigger_executor.CancelTasks();
	if (trigger_executor.HasError()) {
		trigger_executor.ThrowException();
	}
}

void TriggerExecutor::FireAfterInsert(ClientContext &context, TableCatalogEntry &table, idx_t row_count) {
	if (context.trigger_depth >= MAX_TRIGGER_DEPTH) {
		throw InvalidInputException("Trigger recursion depth limit (%llu) exceeded — possible infinite trigger loop",
		                            MAX_TRIGGER_DEPTH);
	}

	// Collecting trigger body strings before executing any of them.
	// The schema.Scan holds catalog_lock for its entire duration.
	// ExecuteTriggerBody may call FireAfterInsert again (recursive triggers),
	// which also tries to acquire catalog_lock => deadlock.
	struct TriggerInfo {
		string body;
		TriggerForEach for_each;
	};
	vector<TriggerInfo> triggers;
	{
		auto &schema = table.ParentSchema();
		schema.Scan(context, CatalogType::TRIGGER_ENTRY, [&](CatalogEntry &entry) {
			auto &trigger = entry.Cast<TriggerCatalogEntry>();
			if (trigger.timing != TriggerTiming::AFTER || trigger.event_type != TriggerEventType::INSERT_EVENT) {
				return;
			}
			if (trigger.base_table->table_name != table.name) {
				return;
			}
			triggers.push_back({trigger.sql_body_text, trigger.for_each});
		});
	}

	TriggerDepthGuard depth_guard(context.trigger_depth);
	for (auto &trigger : triggers) {
		if (trigger.for_each == TriggerForEach::ROW) {
			for (idx_t i = 0; i < row_count; i++) {
				ExecuteTriggerBody(context, trigger.body);
			}
		} else {
			// FOR EACH STATEMENT: fire once regardless of row count
			ExecuteTriggerBody(context, trigger.body);
		}
	}
}

} // namespace duckdb
