#include "duckdb/main/capi_internal.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

using duckdb::DatabaseData;

void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks) {
	if (!database) {
		return;
	}
	auto wrapper = (DatabaseData *)database;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
	scheduler.ExecuteTasks(max_tasks);
}
