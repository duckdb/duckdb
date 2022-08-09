#include "duckdb/main/capi_internal.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

using duckdb::DatabaseData;

duckdb_task duckdb_get_task(duckdb_database database) {
	if (!database) {
		return nullptr;
	}
	auto wrapper = (DatabaseData *)database;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
	auto task = scheduler.GetTask();
	return task.release();
}

void duckdb_execute_task(duckdb_task task_p) {
	if (!task_p) {
		return;
	}
	auto task = (duckdb::Task *) task_p;
	task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
	delete task;
}
