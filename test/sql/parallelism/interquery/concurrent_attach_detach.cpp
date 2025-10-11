#include "catch.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "test_helpers.hpp"

#include <thread>

using namespace duckdb;

namespace {

string test_dir_path;
const string prefix = "db_";
const string suffix = ".db";

string getDBPath(idx_t i) {
	return test_dir_path + "/" + prefix + to_string(i) + suffix;
}

string getDBName(idx_t i) {
	return prefix + to_string(i);
}

const idx_t db_count = 10;
const idx_t worker_count = 40;
const idx_t iteration_count = 100;
const idx_t nr_initial_rows = 2050;

vector<string> logging;
mutex log_mutex;
atomic<bool> success {true};

void addLog(const string &msg) {
	if (success) {
		lock_guard<mutex> lock(log_mutex);
		logging.push_back(msg);
	}
}

duckdb::unique_ptr<MaterializedQueryResult> execQuery(Connection &conn, const string &query) {
	auto result = conn.Query(query);
	if (result->HasError()) {
		Printer::Print(result->GetError());
		success = false;
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

DBInfo db_infos[db_count];

class DBPoolMgr {
public:
	mutex mu;
	map<idx_t, idx_t> m;

	void addWorker(Connection &conn, const idx_t i) {
		lock_guard<mutex> lock(mu);

		if (m.find(i) != m.end()) {
			m[i]++;
			return;
		}

		m[i] = 1;
		string query = "ATTACH '" + getDBPath(i) + "'";
		execQuery(conn, query);
	}

	void removeWorker(Connection &conn, const idx_t i) {
		lock_guard<mutex> lock(mu);

		m[i]--;
		if (m[i] != 0) {
			return;
		}

		m.erase(i);
		string query = "DETACH " + getDBName(i);
		execQuery(conn, query);
	}
};

DBPoolMgr db_pool;

void createTbl(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto tbl_id = db_infos[db_id].table_count;
	db_infos[db_id].tables.emplace_back(TableInfo {nr_initial_rows});
	db_infos[db_id].table_count++;

	string query = "CREATE TABLE " + getDBName(db_id) + ".tbl_" + to_string(tbl_id) +
	               " AS SELECT "
	               "range::UBIGINT AS i, "
	               "range::VARCHAR AS s, "
	               // Note: We increment timestamps by 1 millisecond (i.e., 1000 microseconds).
	               "epoch_ms(range) AS ts, "
	               "{'key1': range::UBIGINT, 'key2': range::VARCHAR} AS obj "
	               "FROM range(" +
	               to_string(nr_initial_rows) + ")";
	addLog("thread: " + to_string(worker_id) + "; q: " + query);
	execQuery(conn, query);
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
	auto table_name = getDBName(db_id) + ".tbl_" + to_string(tbl_id);
	string query = "SELECT i, s, ts, obj FROM " + table_name + " WHERE i = " + to_string(expected_max_val);
	addLog("thread: " + to_string(worker_id) + "; q: " + query);
	auto result = execQuery(conn, query);

	if (!CHECK_COLUMN(result, 0, {Value::UBIGINT(expected_max_val)})) {
		success = false;
		return;
	}
	if (!CHECK_COLUMN(result, 1, {to_string(expected_max_val)})) {
		success = false;
		return;
	}
	if (!CHECK_COLUMN(result, 2, {Value::TIMESTAMP(timestamp_t {static_cast<int64_t>(expected_max_val * 1000)})})) {
		success = false;
		return;
	}
	if (!CHECK_COLUMN(
	        result, 3,
	        {Value::STRUCT({{"key1", Value::UBIGINT(expected_max_val)}, {"key2", to_string(expected_max_val)}})})) {
		success = false;
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
	db_infos[db_id].tables[tbl_id].size += STANDARD_VECTOR_SIZE;

	// set appender
	auto tbl_str = "tbl_" + to_string(tbl_id);
	addLog("thread: " + to_string(worker_id) + "; db: " + getDBName(db_id) + "; table: " + tbl_str + "; append rows");

	try {
		Appender appender(conn, getDBName(db_id), DEFAULT_SCHEMA, tbl_str);
		DataChunk chunk;

		child_list_t<LogicalType> struct_children;
		struct_children.emplace_back(make_pair("key1", LogicalTypeId::UBIGINT));
		struct_children.emplace_back(make_pair("key2", LogicalTypeId::VARCHAR));

		const vector<LogicalType> types = {LogicalType::UBIGINT, LogicalType::VARCHAR, LogicalType::TIMESTAMP,
		                                   LogicalType::STRUCT(struct_children)};

		// fill up datachunk
		chunk.Initialize(*conn.context, types);
		// int
		auto &col_ubigint = chunk.data[0];
		auto data_ubigint = FlatVector::GetData<uint64_t>(col_ubigint);
		// varchar
		auto &col_varchar = chunk.data[1];
		auto data_varchar = FlatVector::GetData<string_t>(col_varchar);
		// timestamp
		auto &col_ts = chunk.data[2];
		auto data_ts = FlatVector::GetData<timestamp_t>(col_ts);
		// struct
		auto &col_struct = chunk.data[3];
		auto &data_struct_entries = StructVector::GetEntries(col_struct);
		auto &entry_ubigint = data_struct_entries[0];
		auto data_struct_ubigint = FlatVector::GetData<uint64_t>(*entry_ubigint);
		auto &entry_varchar = data_struct_entries[1];
		auto data_struct_varchar = FlatVector::GetData<string_t>(*entry_varchar);

		for (idx_t row_idx = 0; row_idx < STANDARD_VECTOR_SIZE; row_idx++) {
			data_ubigint[row_idx] = current_num_rows + row_idx;
			data_varchar[row_idx] = StringVector::AddString(col_varchar, to_string(current_num_rows + row_idx));
			data_ts[row_idx] = timestamp_t {static_cast<int64_t>(1000 * (current_num_rows + row_idx))};
			data_struct_ubigint[row_idx] = current_num_rows + row_idx;
			data_struct_varchar[row_idx] =
			    StringVector::AddString(*entry_varchar, to_string(current_num_rows + row_idx));
		}

		chunk.SetCardinality(STANDARD_VECTOR_SIZE);
		appender.AppendDataChunk(chunk);
		appender.Close();

	} catch (const std::exception &e) {
		addLog("Caught exception when using Appender: " + string(e.what()));
		success = false;
		return;
	} catch (...) {
		addLog("Caught error when using Appender!");
		success = false;
		return;
	}
}

void workUnit(std::unique_ptr<Connection> conn, const idx_t worker_id) {
	for (idx_t i = 0; i < iteration_count; i++) {
		if (!success) {
			return;
		}

		try {
			idx_t scenario_id = std::rand() % 3;
			idx_t db_id = std::rand() % db_count;

			db_pool.addWorker(*conn, db_id);

			switch (scenario_id) {
			case 0:
				createTbl(*conn, db_id, worker_id);
				break;
			case 1:
				lookup(*conn, db_id, worker_id);
				break;
			case 2:
				append(*conn, db_id, worker_id);
				break;
			default:
				addLog("invalid scenario: " + to_string(scenario_id));
				success = false;
				return;
			}
			db_pool.removeWorker(*conn, db_id);

		} catch (const std::exception &e) {
			addLog("Caught exception when running iterations: " + string(e.what()));
			success = false;
			return;
		} catch (...) {
			addLog("Caught unknown when using running iterations");
			success = false;
			return;
		}
	}
}

TEST_CASE("Run a concurrent ATTACH/DETACH scenario", "[interquery][.]") {
	test_dir_path = TestDirectoryPath();

	DuckDB db(nullptr);
	Connection init_conn(db);

	execQuery(init_conn, "SET catalog_error_max_schemas = '0'");
	execQuery(init_conn, "SET threads = '1'");
	execQuery(init_conn, "SET storage_compatibility_version = 'v1.3.2'");

	vector<std::thread> workers;
	for (idx_t i = 0; i < worker_count; i++) {
		auto conn = make_uniq<Connection>(db);
		workers.emplace_back(workUnit, std::move(conn), i);
	}

	for (auto &worker : workers) {
		worker.join();
	}
	if (!success) {
		for (const auto &msg : logging) {
			Printer::Print(msg);
		}
		FAIL();
	}
}

TEST_CASE("Test FORCE DETACH syntax and protection", "[interquery][.]") {
	DuckDB db(nullptr);
	Connection conn(db);

	string db_path = TestCreatePath("force_detach_test.db");

	// Test 1: FORCE DETACH syntax works and database is removed from schema
	REQUIRE_NO_FAIL(conn.Query("ATTACH '" + db_path + "' AS test_db"));

	// Verify database appears in schema
	auto result = conn.Query("SELECT database_name FROM duckdb_databases() WHERE database_name = 'test_db'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {"test_db"}));

	REQUIRE_NO_FAIL(conn.Query("CREATE TABLE test_db.test_table(i INTEGER)"));
	REQUIRE_NO_FAIL(conn.Query("INSERT INTO test_db.test_table VALUES (1), (2), (3)"));
	REQUIRE_NO_FAIL(conn.Query("FORCE DETACH test_db"));

	// Verify database is gone from schema
	result = conn.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_db'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// Test 2: DETACH fails with active transaction, then succeeds after commit
	REQUIRE_NO_FAIL(conn.Query("ATTACH '" + db_path + "' AS test_db"));
	REQUIRE_NO_FAIL(conn.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(conn.Query("SELECT * FROM test_db.test_table"));

	// Regular DETACH should fail - transaction is active
	result = conn.Query("DETACH test_db");
	REQUIRE(result->HasError());
	REQUIRE(result->GetError().find("still in use") != string::npos);

	// Commit the transaction
	REQUIRE_NO_FAIL(conn.Query("COMMIT"));

	// Now DETACH should work
	REQUIRE_NO_FAIL(conn.Query("DETACH test_db"));

	// Verify database is gone from schema
	result = conn.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_db'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// Test 3: Verify FORCE DETACH with all syntax variations and schema removal
	REQUIRE_NO_FAIL(conn.Query("ATTACH '" + db_path + "' AS test_db1"));
	REQUIRE_NO_FAIL(conn.Query("FORCE DETACH test_db1"));
	result = conn.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_db1'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	REQUIRE_NO_FAIL(conn.Query("ATTACH '" + db_path + "' AS test_db2"));
	REQUIRE_NO_FAIL(conn.Query("FORCE DETACH DATABASE test_db2"));
	result = conn.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_db2'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	REQUIRE_NO_FAIL(conn.Query("ATTACH '" + db_path + "' AS test_db3"));
	REQUIRE_NO_FAIL(conn.Query("FORCE DETACH DATABASE IF EXISTS test_db3"));
	result = conn.Query("SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'test_db3'");
	REQUIRE_NO_FAIL(result);
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// Test IF EXISTS with non-existent database (should not fail)
	REQUIRE_NO_FAIL(conn.Query("FORCE DETACH DATABASE IF EXISTS nonexistent_db"));
}

} // anonymous namespace
