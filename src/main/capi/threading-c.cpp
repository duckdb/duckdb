#include "duckdb/main/capi_internal.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

using duckdb::DatabaseData;

struct CAPITaskState {
	CAPITaskState(duckdb::DatabaseInstance &db)
	    : db(db), marker(duckdb::make_unique<duckdb::atomic<bool>>(true)), execute_count(0) {
	}

	duckdb::DatabaseInstance &db;
	duckdb::unique_ptr<duckdb::atomic<bool>> marker;
	duckdb::atomic<idx_t> execute_count;
};

void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks) {
	if (!database) {
		return;
	}
	auto wrapper = (DatabaseData *)database;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
	scheduler.ExecuteTasks(max_tasks);
}

duckdb_task_state duckdb_create_task_state(duckdb_database database) {
	if (!database) {
		return nullptr;
	}
	auto wrapper = (DatabaseData *)database;
	auto state = new CAPITaskState(*wrapper->database->instance);
	return state;
}

void duckdb_execute_tasks_state(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
	state->execute_count++;
	scheduler.ExecuteForever(state->marker.get());
}

void duckdb_finish_execution(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	*state->marker = false;
	if (state->execute_count > 0) {
		// signal to the threads to wake up
		auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
		scheduler.Signal(state->execute_count);
	}
}

void duckdb_destroy_task_state(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	delete state;
}
