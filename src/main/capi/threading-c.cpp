#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

using duckdb::DatabaseWrapper;

struct CAPITaskState {
	explicit CAPITaskState(duckdb::DatabaseInstance &db)
	    : db(db), marker(duckdb::make_uniq<duckdb::atomic<bool>>(true)), execute_count(0) {
	}

	duckdb::DatabaseInstance &db;
	duckdb::unique_ptr<duckdb::atomic<bool>> marker;
	duckdb::atomic<idx_t> execute_count;
};

void duckdb_execute_tasks(duckdb_database database, idx_t max_tasks) {
	if (!database) {
		return;
	}
	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
	scheduler.ExecuteTasks(max_tasks);
}

duckdb_task_state duckdb_create_task_state(duckdb_database database) {
	if (!database) {
		return nullptr;
	}
	auto wrapper = reinterpret_cast<DatabaseWrapper *>(database);
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

idx_t duckdb_execute_n_tasks_state(duckdb_task_state state_p, idx_t max_tasks) {
	if (!state_p) {
		return 0;
	}
	auto state = (CAPITaskState *)state_p;
	auto &scheduler = duckdb::TaskScheduler::GetScheduler(state->db);
	return scheduler.ExecuteTasks(state->marker.get(), max_tasks);
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

bool duckdb_task_state_is_finished(duckdb_task_state state_p) {
	if (!state_p) {
		return false;
	}
	auto state = (CAPITaskState *)state_p;
	return !(*state->marker);
}

void duckdb_destroy_task_state(duckdb_task_state state_p) {
	if (!state_p) {
		return;
	}
	auto state = (CAPITaskState *)state_p;
	delete state;
}

bool duckdb_execution_is_finished(duckdb_connection con) {
	if (!con) {
		return false;
	}
	duckdb::Connection *conn = (duckdb::Connection *)con;
	return conn->context->ExecutionIsFinished();
}
