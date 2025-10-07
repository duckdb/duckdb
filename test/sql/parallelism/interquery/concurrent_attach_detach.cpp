#include "catch.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "test_helpers.hpp"

#include <unordered_set>
#include <thread>

using namespace duckdb;

namespace {

string TEST_DIR_PATH;
const string PREFIX = "db_";
const string SUFFIX = ".db";

string get_db_path(idx_t i) {
	return TEST_DIR_PATH + "/" + PREFIX + to_string(i) + SUFFIX;
}

string get_db_name(idx_t i) {
	return PREFIX + to_string(i);
}

constexpr idx_t DB_COUNT = 10;
constexpr idx_t WORKER_COUNT = 40;
constexpr idx_t ITERATION_COUNT = 400;
constexpr idx_t NR_INITIAL_ROWS = 2050;
constexpr idx_t PROFILING_INTERVAL = 10;

// Set to true to enable interruptions of workers.
constexpr bool INTERRUPT_WORKERS = false;

vector<string> LOGGING[WORKER_COUNT] = {{}};
atomic<bool> IS_SUCCESS {true};
idx_t QUERY_COUNT[WORKER_COUNT] = {0};
bool ACTIVE_TRANSACTION[WORKER_COUNT] = {false};

vector<std::unique_ptr<Connection>> CONNECTIONS;
atomic<idx_t> ACTIVE_WORKERS {0};

// Queries that currently don't support profiling.
vector<string> SKIP_PROFILING_QUERIES = {"ROLLBACK"};

void add_log(const idx_t worker_id, const string &msg) {
	LOGGING[worker_id].push_back(msg);
}

duckdb::unique_ptr<MaterializedQueryResult> exec_query(Connection &conn, const string &query, const idx_t worker_id) {
	QUERY_COUNT[worker_id]++;

	// Enable profiling for every 10th query, unless it's in the skip list.
	auto profiling_enabled = QUERY_COUNT[worker_id] % PROFILING_INTERVAL == 0;
	if (profiling_enabled && std::find(SKIP_PROFILING_QUERIES.begin(), SKIP_PROFILING_QUERIES.end(), query) !=
	                             SKIP_PROFILING_QUERIES.end()) {
		profiling_enabled = false;
	}

	if (profiling_enabled) {
		add_log(worker_id, "Enabling profiling");
		conn.EnableProfiling();
		conn.Query("PRAGMA enable_profiling = 'no_output'");
		conn.Query("SET profiling_coverage = 'ALL'");
	}

	auto result = conn.Query(query);
	if (result->HasError() && result->GetErrorType() != ExceptionType::INTERRUPT) {
		Printer::PrintF("Failed to execute query %s:\n------\n%s\n-------", query, result->GetError());
		IS_SUCCESS = false;
	}

	if (profiling_enabled) {
		add_log(worker_id, "Retrieving profiling info for " + query);
		auto profile = conn.GetProfilingTree();
		if (!profile) {
			Printer::PrintF("Failed to get profiling info for query %s", query);
			IS_SUCCESS = false;
		}

		auto profiled_query_name = profile->GetProfilingInfo().GetMetricAsString(MetricsType::QUERY_NAME);
		if (profiled_query_name != query) {
			Printer::PrintF("Profiling info query name does not match executed query: '%s' != '%s'",
			                profiled_query_name, query);
			IS_SUCCESS = false;
		}

		conn.DisableProfiling();
	}

	return result;
}

struct TableInfo {
	idx_t size;
};

struct DBInfo {
	mutex mu;
	idx_t table_count = 0;
	vector<TableInfo> tables;
};

void interrupt_attach(Connection &conn, const idx_t worker_id) {
	add_log(worker_id, "Interrupting connection");
	conn.Interrupt();
}

DBInfo db_infos[DB_COUNT];

class DBPoolMgr {
public:
	mutex mu;
	map<idx_t, idx_t> m;

	void add_worker(Connection &conn, const idx_t db_id, const idx_t worker_id) {
		lock_guard<mutex> lock(mu);

		if (m.find(db_id) != m.end()) {
			m[db_id]++;
		} else {
			m[db_id] = 1;
		}

		if (rand() % 2 == 0) {
			// 50% chance to spawn an interruption worker
			std::thread interruption(interrupt_attach, std::ref(conn), worker_id);
			interruption.join();
		}

		string query = "ATTACH IF NOT EXISTS'" + get_db_path(db_id) + "'";
		exec_query(conn, query, worker_id);
	}

	void remove_worker(Connection &conn, const idx_t db_id, const idx_t worker_id) {
		lock_guard<mutex> lock(mu);

		m[db_id]--;
		if (m[db_id] != 0) {
			return;
		}

		m.erase(db_id);
		string query = "DETACH " + get_db_name(db_id);
		exec_query(conn, query, worker_id);
	}
};

DBPoolMgr db_pool;

void create_tbl(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto tbl_id = db_infos[db_id].table_count;
	db_infos[db_id].tables.emplace_back(TableInfo {NR_INITIAL_ROWS});
	db_infos[db_id].table_count++;

	// Create the table.
	string tbl_path = StringUtil::Format("%s.tbl_%d", get_db_name(db_id), tbl_id);
	string create_sql = StringUtil::Format(
	    "CREATE TABLE %s(i BIGINT PRIMARY KEY, s VARCHAR, ts TIMESTAMP, obj STRUCT(key1 UBIGINT, key2 VARCHAR))",
	    tbl_path);
	add_log(worker_id, "; q: " + create_sql);
	exec_query(conn, create_sql, worker_id);

	// Insert initial rows.
	string insert_sql = "INSERT INTO " + tbl_path +
	                    " SELECT "
	                    "range::UBIGINT AS i, "
	                    "range::VARCHAR AS s, "
	                    // Note: We increment timestamps by 1 millisecond (i.e., 1000 microseconds).
	                    "epoch_ms(range) AS ts, "
	                    "{'key1': range::UBIGINT, 'key2': range::VARCHAR} AS obj "
	                    "FROM range(" +
	                    to_string(NR_INITIAL_ROWS) + ")";
	add_log(worker_id, "; q: " + insert_sql);
	exec_query(conn, insert_sql, worker_id);
}

void lookup(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	unique_lock<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;

	if (max_tbl_id == 0) {
		lock.unlock();
		return;
	}

	auto tbl_id = std::rand() % max_tbl_id;
	auto expected_max_val = db_infos[db_id].tables[tbl_id].size - 1;
	lock.unlock();

	// Run the query.
	auto table_name = get_db_name(db_id) + ".tbl_" + to_string(tbl_id);
	string query = "SELECT i, s, ts, obj FROM " + table_name + " WHERE i = " + to_string(expected_max_val);
	add_log(worker_id, "q: " + query);

	// Verify the results.
	auto result = exec_query(conn, query, worker_id);
	if (result->RowCount() == 0) {
		Printer::PrintF("FAILURE - No rows returned from query");
		IS_SUCCESS = false;
	}
	if (!CHECK_COLUMN(result, 0, {Value::UBIGINT(expected_max_val)})) {
		IS_SUCCESS = false;
		return;
	}
	if (!CHECK_COLUMN(result, 1, {to_string(expected_max_val)})) {
		IS_SUCCESS = false;
		return;
	}
	if (!CHECK_COLUMN(result, 2, {Value::TIMESTAMP(timestamp_t {static_cast<int64_t>(expected_max_val * 1000)})})) {
		IS_SUCCESS = false;
		return;
	}
	if (!CHECK_COLUMN(
	        result, 3,
	        {Value::STRUCT({{"key1", Value::UBIGINT(expected_max_val)}, {"key2", to_string(expected_max_val)}})})) {
		IS_SUCCESS = false;
		return;
	}
}

void append_internal(Connection &conn, const idx_t db_id, const idx_t tbl_id, const idx_t worker_id,
                     const vector<idx_t> &ids) {
	// Log appender command.
	auto tbl_str = "tbl_" + to_string(tbl_id);
	add_log(worker_id, "db: " + get_db_name(db_id) + "; table: " + tbl_str + "; append rows");

	try {
		Appender appender(conn, get_db_name(db_id), DEFAULT_SCHEMA, tbl_str);
		DataChunk chunk;

		child_list_t<LogicalType> struct_children;
		struct_children.emplace_back(make_pair("key1", LogicalTypeId::UBIGINT));
		struct_children.emplace_back(make_pair("key2", LogicalTypeId::VARCHAR));

		const vector<LogicalType> types = {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::TIMESTAMP,
		                                   LogicalType::STRUCT(struct_children)};

		chunk.Initialize(*conn.context, types);
		// UBIGINT
		auto &col_ubigint = chunk.data[0];
		auto data_ubigint = FlatVector::GetData<uint64_t>(col_ubigint);
		// VARCHAR
		auto &col_varchar = chunk.data[1];
		auto data_varchar = FlatVector::GetData<string_t>(col_varchar);
		// TIMESTAMP
		auto &col_ts = chunk.data[2];
		auto data_ts = FlatVector::GetData<timestamp_t>(col_ts);
		// STRUCT(UBIGINT, VARCHAR)
		auto &col_struct = chunk.data[3];
		auto &data_struct_entries = StructVector::GetEntries(col_struct);
		auto &entry_ubigint = data_struct_entries[0];
		auto data_struct_ubigint = FlatVector::GetData<uint64_t>(*entry_ubigint);
		auto &entry_varchar = data_struct_entries[1];
		auto data_struct_varchar = FlatVector::GetData<string_t>(*entry_varchar);

		for (idx_t i = 0; i < ids.size(); i++) {
			auto row_idx = ids[i];
			data_ubigint[i] = row_idx;
			data_varchar[i] = StringVector::AddString(col_varchar, to_string(row_idx));
			data_ts[i] = timestamp_t {static_cast<int64_t>(1000 * (row_idx))};
			data_struct_ubigint[i] = row_idx;
			data_struct_varchar[i] = StringVector::AddString(*entry_varchar, to_string(row_idx));
		}

		chunk.SetCardinality(ids.size());
		appender.AppendDataChunk(chunk);
		appender.Close();

	} catch (const std::exception &e) {
		add_log(worker_id, "Caught exception when using Appender: " + string(e.what()));
		IS_SUCCESS = false;
		return;
	} catch (...) {
		add_log(worker_id, "Caught unknown exception when using Appender");
		IS_SUCCESS = false;
		return;
	}
}

void append(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}

	auto tbl_id = std::rand() % max_tbl_id;
	auto current_num_rows = db_infos[db_id].tables[tbl_id].size;
	idx_t append_count = STANDARD_VECTOR_SIZE;

	vector<idx_t> ids;
	for (idx_t i = 0; i < append_count; i++) {
		ids.push_back(current_num_rows + i);
	}

	append_internal(conn, db_id, tbl_id, worker_id, ids);
	db_infos[db_id].tables[tbl_id].size += append_count;
}

void delete_internal(Connection &conn, const idx_t db_id, const idx_t tbl_id, const idx_t worker_id,
                     const vector<idx_t> &ids) {
	auto tbl_str = "tbl_" + to_string(tbl_id);

	string delete_list;
	for (auto delete_idx : ids) {
		if (!delete_list.empty()) {
			delete_list += ", ";
		}
		delete_list += "(" + to_string(delete_idx) + ")";
	}
	string delete_sql =
	    StringUtil::Format("WITH ids (id) AS (VALUES %s) DELETE FROM %s.%s.%s AS t USING ids WHERE t.i = ids.id",
	                       delete_list, get_db_name(db_id), DEFAULT_SCHEMA, tbl_str);
	add_log(worker_id, "q: " + delete_sql);
	exec_query(conn, delete_sql, worker_id);
}

void apply_changes(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}

	// Select a random table to delete from.
	auto tbl_id = std::rand() % max_tbl_id;

	// Select some random tuples to apply changes to.
	auto current_num_rows = db_infos[db_id].tables[tbl_id].size;
	idx_t delete_count = std::rand() % (STANDARD_VECTOR_SIZE / 3);
	if (delete_count == 0) {
		delete_count = 1;
	}
	unordered_set<idx_t> unique_ids;
	for (idx_t i = 0; i < delete_count; i++) {
		unique_ids.insert(std::rand() % current_num_rows);
	}
	vector<idx_t> ids;
	for (auto &id : unique_ids) {
		ids.push_back(id);
	}

	// Apply the changes.
	ACTIVE_TRANSACTION[worker_id] = true;
	exec_query(conn, "BEGIN", worker_id);
	delete_internal(conn, db_id, tbl_id, worker_id, ids);
	append_internal(conn, db_id, tbl_id, worker_id, ids);

	// Randomly COMMIT or ROLLBACK.
	auto commit = std::rand() % 2 == 0;
	if (commit) {
		exec_query(conn, "COMMIT", worker_id);
	} else {
		exec_query(conn, "ROLLBACK", worker_id);
	}
	ACTIVE_TRANSACTION[worker_id] = false;
}

void describe_tbl(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	unique_lock<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}

	auto tbl_id = std::rand() % max_tbl_id;
	auto tbl_str = "tbl_" + to_string(tbl_id);
	lock.unlock();

	auto actual_describe = std::rand() % 2 == 0;
	string describe_sql;
	if (actual_describe) {
		describe_sql = StringUtil::Format("DESCRIBE %s.%s.%s", get_db_name(db_id), DEFAULT_SCHEMA, tbl_str);
	} else {
		describe_sql =
		    StringUtil::Format("SELECT 1 FROM %s.%s.%s LIMIT 1", get_db_name(db_id), DEFAULT_SCHEMA, tbl_str);
	}

	add_log(worker_id, "q: " + describe_sql);
	exec_query(conn, describe_sql, worker_id);
}

void work_unit(const idx_t worker_id) {
	auto &conn = *CONNECTIONS.at(worker_id);

	for (idx_t i = 0; i < ITERATION_COUNT; i++) {
		if (!IS_SUCCESS) {
			return;
		}

		try {
			idx_t scenario_id = std::rand() % 9;
			idx_t db_id = std::rand() % DB_COUNT;

			db_pool.add_worker(conn, db_id, worker_id);

			switch (scenario_id) {
			case 0:
				create_tbl(conn, db_id, worker_id);
				break;
			case 1:
				lookup(conn, db_id, worker_id);
				break;
			case 2:
				append(conn, db_id, worker_id);
				break;
			case 3:
				apply_changes(conn, db_id, worker_id);
				break;
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
				describe_tbl(conn, db_id, worker_id);
				break;
			default:
				add_log(worker_id, "invalid scenario: " + to_string(scenario_id));
				IS_SUCCESS = false;
				return;
			}
			db_pool.remove_worker(conn, db_id, worker_id);

		} catch (const std::exception &e) {
			add_log(worker_id, "Caught exception when running iterations: " + string(e.what()));
			IS_SUCCESS = false;
			return;
		} catch (...) {
			add_log(worker_id, "Caught unknown when using running iterations");
			IS_SUCCESS = false;
			return;
		}
	}

	--ACTIVE_WORKERS;
}

void worker_interruptor() {
	while (ACTIVE_WORKERS > 0 || IS_SUCCESS) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));

		auto worker_id = std::rand() % WORKER_COUNT;
		auto &conn = *CONNECTIONS.at(worker_id);
		if (ACTIVE_TRANSACTION[worker_id]) {
			Printer::PrintF("INTERRUPTING worker %d\n", worker_id);
			add_log(worker_id, "Interrupting connection");
			conn.Interrupt();
			conn.Rollback();
		}
	}
}

TEST_CASE("Run a concurrent ATTACH/DETACH scenario", "[interquery][.]") {
	TEST_DIR_PATH = TestDirectoryPath();

	DuckDB db(nullptr);
	Connection init_conn(db);

	exec_query(init_conn, "SET catalog_error_max_schemas = '0'", 0);
	exec_query(init_conn, "SET threads = '1'", 0);
	exec_query(init_conn, "SET storage_compatibility_version = 'latest'", 0);
	exec_query(init_conn, "CALL enable_logging()", 0);
	exec_query(init_conn, "SET default_block_size = 16384", 0);

	// Spawn workers.
	vector<std::thread> workers;
	for (idx_t worker_id = 0; worker_id < WORKER_COUNT; worker_id++) {
		auto conn = make_uniq<Connection>(db);
		CONNECTIONS.push_back(std::move(conn));
		workers.emplace_back(work_unit, worker_id);
	}

	// Start the interrupter.
	if (INTERRUPT_WORKERS) {
		std::thread thread_interrupt(worker_interruptor);
		thread_interrupt.join();
	}

	for (auto &worker : workers) {
		worker.join();
	}
	if (!IS_SUCCESS) {
		for (idx_t worker_id = 0; worker_id < WORKER_COUNT; worker_id++) {
			for (auto &log : LOGGING[worker_id]) {
				if (log.find("exception_type") != string::npos) {
					// Make exceptions more visible.
					Printer::PrintF("\n\n====================================================\n\n");
					Printer::PrintF("thread %d; %s", worker_id, log);
					Printer::PrintF("\n\n====================================================\n\n");
				} else {
					Printer::PrintF("thread %d; %s", worker_id, log);
				}
			}
		}
		FAIL();
	}
}

} // anonymous namespace
