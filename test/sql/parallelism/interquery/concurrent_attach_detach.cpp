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

string test_dir_path;
const string prefix = "db_";
const string suffix = ".db";

string getDBPath(idx_t i) {
	return test_dir_path + "/" + prefix + to_string(i) + suffix;
}

string getDBName(idx_t i) {
	return prefix + to_string(i);
}

const idx_t db_count = 100;
const idx_t worker_count = 50;
const idx_t iteration_count = 500;
const idx_t nr_initial_rows = 2050;

vector<vector<string>> logging;
atomic<bool> success {true};

void addLog(const idx_t worker_id, const string &msg) {
	logging[worker_id].push_back(msg);
}

duckdb::unique_ptr<MaterializedQueryResult> execQuery(Connection &conn, const string &query) {
	auto result = conn.Query(query);
	if (result->HasError()) {
		Printer::PrintF("Failed to execute query %s:\n------\n%s\n-------", query, result->GetError());
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

	string tbl_path = StringUtil::Format("%s.tbl_%d", getDBName(db_id), tbl_id);
	string create_sql = StringUtil::Format(
	    "CREATE TABLE %s(i BIGINT PRIMARY KEY, s VARCHAR, ts TIMESTAMP, obj STRUCT(key1 UBIGINT, key2 VARCHAR))",
	    tbl_path);
	addLog(worker_id, "; q: " + create_sql);
	execQuery(conn, create_sql);
	string insert_sql = "INSERT INTO " + tbl_path +
	                    " SELECT "
	                    "range::UBIGINT AS i, "
	                    "range::VARCHAR AS s, "
	                    // Note: We increment timestamps by 1 millisecond (i.e., 1000 microseconds).
	                    "epoch_ms(range) AS ts, "
	                    "{'key1': range::UBIGINT, 'key2': range::VARCHAR} AS obj "
	                    "FROM range(" +
	                    to_string(nr_initial_rows) + ")";
	addLog(worker_id, "; q: " + insert_sql);
	execQuery(conn, insert_sql);
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
	addLog(worker_id, "q: " + query);
	auto result = execQuery(conn, query);
	if (result->RowCount() == 0) {
		Printer::PrintF("FAILURE - No rows returned from query");
		success = false;
	}
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

void append_internal(Connection &conn, const idx_t db_id, const idx_t tbl_id, const idx_t worker_id,
                     const vector<idx_t> &ids) {
	auto tbl_str = "tbl_" + to_string(tbl_id);
	// set appender
	addLog(worker_id, "db: " + getDBName(db_id) + "; table: " + tbl_str + "; append rows");

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
		addLog(worker_id, "Caught exception when using Appender: " + string(e.what()));
		success = false;
		return;
	} catch (...) {
		addLog(worker_id, "Caught error when using Appender!");
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
	                       delete_list, getDBName(db_id), DEFAULT_SCHEMA, tbl_str);
	addLog(worker_id, "q: " + delete_sql);
	execQuery(conn, delete_sql);
}

void apply_changes(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}
	// select a random table to delete from
	auto tbl_id = std::rand() % max_tbl_id;
	// select some random tuples to apply changes to
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
	execQuery(conn, "BEGIN");
	delete_internal(conn, db_id, tbl_id, worker_id, ids);
	append_internal(conn, db_id, tbl_id, worker_id, ids);
	execQuery(conn, "COMMIT");
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
		describe_sql = StringUtil::Format("DESCRIBE %s.%s.%s", getDBName(db_id), DEFAULT_SCHEMA, tbl_str);
	} else {
		describe_sql = StringUtil::Format("SELECT 1 FROM %s.%s.%s LIMIT 1", getDBName(db_id), DEFAULT_SCHEMA, tbl_str);
	}

	addLog(worker_id, "q: " + describe_sql);
	execQuery(conn, describe_sql);
}

void checkpoint_db(Connection &conn, const idx_t db_id, const idx_t worker_id) {
	unique_lock<mutex> lock(db_infos[db_id].mu);
	string checkpoint_sql = "CHECKPOINT " + getDBName(db_id);
	addLog(worker_id, "q: " + checkpoint_sql);
	execQuery(conn, checkpoint_sql);
}

void workUnit(std::unique_ptr<Connection> conn, const idx_t worker_id) {
	for (idx_t i = 0; i < iteration_count; i++) {
		if (!success) {
			return;
		}

		try {
			idx_t scenario_id = std::rand() % 10;
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
			case 3:
				apply_changes(*conn, db_id, worker_id);
				break;
			case 4:
			case 5:
			case 6:
			case 7:
			case 8:
				describe_tbl(*conn, db_id, worker_id);
				break;
			case 9:
				checkpoint_db(*conn, db_id, worker_id);
				break;
			default:
				addLog(worker_id, "invalid scenario: " + to_string(scenario_id));
				success = false;
				return;
			}
			db_pool.removeWorker(*conn, db_id);

		} catch (const std::exception &e) {
			addLog(worker_id, "Caught exception when running iterations: " + string(e.what()));
			success = false;
			return;
		} catch (...) {
			addLog(worker_id, "Caught unknown when using running iterations");
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
	execQuery(init_conn, "SET storage_compatibility_version = 'latest'");
	execQuery(init_conn, "CALL enable_logging()");
	execQuery(init_conn, "PRAGMA enable_profiling='no_output'");

	logging.resize(worker_count);
	vector<std::thread> workers;
	for (idx_t i = 0; i < worker_count; i++) {
		auto conn = make_uniq<Connection>(db);
		workers.emplace_back(workUnit, std::move(conn), i);
	}

	for (auto &worker : workers) {
		worker.join();
	}
	if (!success) {
		for (idx_t worker_id = 0; worker_id < logging.size(); worker_id++) {
			for (auto &log : logging[worker_id]) {
				Printer::PrintF("thread %d; %s", worker_id, log);
			}
		}
		FAIL();
	}
}

} // anonymous namespace
