#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"

#include <string>
#include <vector>
#include <thread>

using namespace duckdb;

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

std::vector<string> logging;
mutex log_mutex;
atomic<bool> success {true};

void addLog(const string &msg) {
	if (success) {
		lock_guard<mutex> lock(log_mutex);
		logging.push_back(msg);
	}
}

unique_ptr<MaterializedQueryResult> execQuery(Connection &conn, const string &query) {
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
	std::vector<TableInfo> tables;
};

DBInfo db_infos[db_count];

class DBPoolMgr {
public:
	mutex mu;
	map<idx_t, idx_t> m;

	void addWorker(Connection &conn, idx_t i) {
		lock_guard<mutex> lock(mu);

		if (m.find(i) != m.end()) {
			m[i]++;
			return;
		}

		m[i] = 1;
		string query = "ATTACH '" + getDBPath(i) + "'";
		execQuery(conn, query);
	}

	void removeWorker(Connection &conn, idx_t i) {
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

void createTbl(Connection &conn, idx_t db_id, idx_t workerId) {
	idx_t nr_initial_rows = 10000;

	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto tblId = db_infos[db_id].table_count;
	db_infos[db_id].tables.emplace_back(TableInfo {nr_initial_rows});
	db_infos[db_id].table_count++;

	string query = "CREATE TABLE " + getDBName(db_id) + ".tbl_" + to_string(tblId) +
	               " AS SELECT "
	               "range::BIGINT AS i, "
	               "range::VARCHAR AS s, "
	               // note: timestamps in this test increment with 1 millisecond (i.e. 1000 microsecond)
	               "epoch_ms(range) AS ts, "
	               "{'key1': range::BIGINT, 'key2': range::VARCHAR} AS obj "
	               "FROM range(" +
	               to_string(nr_initial_rows) + ")";
	addLog("thread: " + to_string(workerId) + "; q: " + query);
	execQuery(conn, query);
}

void lookup(Connection &conn, idx_t db_id, idx_t worker_id) {
	unique_lock<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}
	auto tbl_id = std::rand() % max_tbl_id;
	auto expected_max_val = db_infos[db_id].tables[tbl_id].size - 1;

	// run query
	auto table_name = getDBName(db_id) + ".tbl_" + to_string(tbl_id);
	string query = "SELECT i, s, ts, obj FROM " + table_name + " WHERE i = (select max(i) from " + table_name + ")";
	addLog("thread: " + to_string(worker_id) + "; q: " + query);
	auto result = execQuery(conn, query);
	lock.unlock();

	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(expected_max_val)}));
	REQUIRE(CHECK_COLUMN(result, 1, {to_string(expected_max_val)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::TIMESTAMP(timestamp_t {static_cast<int64_t>(expected_max_val * 1000)})}));
	REQUIRE(CHECK_COLUMN(
	    result, 3, {Value::STRUCT({{"key1", Value::INTEGER(expected_max_val)}, {"key2", to_string(expected_max_val)}})}));
}

void append(Connection &conn, idx_t db_id, idx_t worker_id, idx_t append_num_rows = STANDARD_VECTOR_SIZE) {
	lock_guard<mutex> lock(db_infos[db_id].mu);
	auto max_tbl_id = db_infos[db_id].table_count;
	if (max_tbl_id == 0) {
		return;
	}
	auto tbl_id = std::rand() % max_tbl_id;
	auto current_num_rows = db_infos[db_id].tables[tbl_id].size;
	db_infos[db_id].tables[tbl_id].size += append_num_rows;

	// set appender
	auto tbl_str = "tbl_" + to_string(tbl_id);
	addLog("thread: " + to_string(worker_id) + "; db: " + getDBName(db_id) + "; table: " + tbl_str + "; append rows");
	try {
		duckdb::Appender appender(conn, getDBName(db_id), DEFAULT_SCHEMA, tbl_str);
		DataChunk chunk;

		child_list_t<LogicalType> struct_children;
		struct_children.emplace_back(make_pair("key1", LogicalTypeId::BIGINT));
		struct_children.emplace_back(make_pair("key2", LogicalTypeId::VARCHAR));

		const duckdb::vector<LogicalType> types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::TIMESTAMP,
		                                           LogicalType::STRUCT(struct_children)};

		// fill up datachunk
		chunk.Initialize(*conn.context, types);
		// int
		auto &col_int = chunk.data[0];
		auto data_int = FlatVector::GetData<int64_t>(col_int);
		// varchar
		auto &col_varchar = chunk.data[1];
		auto data_varchar = FlatVector::GetData<string_t>(col_varchar);
		// timestamp
		auto &col_ts = chunk.data[2];
		auto data_ts = FlatVector::GetData<timestamp_t>(col_ts);
		// struct
		auto &col_struct = chunk.data[3];
		auto &data_struct_entries = StructVector::GetEntries(col_struct);
		auto &entry_int = data_struct_entries[0];
		entry_int->SetVectorType(VectorType::FLAT_VECTOR);
		auto data_struct_int = FlatVector::GetData<int64_t>(*entry_int);
		auto &entry_varchar = data_struct_entries[1];
		entry_varchar->SetVectorType(VectorType::FLAT_VECTOR);
		auto data_struct_varchar = FlatVector::GetData<string_t>(*entry_varchar);

		for (idx_t row_idx = 0; row_idx < append_num_rows; row_idx++) {
			data_int[row_idx] = current_num_rows + row_idx;
			data_varchar[row_idx] = StringVector::AddString(col_varchar, to_string(current_num_rows + row_idx));
			data_ts[row_idx] = timestamp_t {static_cast<int64_t>(1000 * (current_num_rows + row_idx))};
			data_struct_int[row_idx] = current_num_rows + row_idx;
			data_struct_varchar[row_idx] =
			    StringVector::AddString(*entry_varchar, to_string(current_num_rows + row_idx));
		}

		chunk.SetCardinality(append_num_rows);
		appender.AppendDataChunk(chunk);
		appender.Close();
	} catch (const std::exception &e) {
		addLog("Caught exception when using Appender: " + std::string(e.what()));
		success = false;
		throw;
	} catch (...) {
		addLog("Caught error when using Appender!");
		success = false;
		throw;
	}
}

void workUnit(std::unique_ptr<Connection> conn, const idx_t &worker_id) {
	for (int i = 0; i < iteration_count; i++) {
		if (!success) {
			break;
		}
		try {
			idx_t scenarioId = std::rand() % 3;
			idx_t dbId = std::rand() % db_count;

			db_pool.addWorker(*conn, dbId);

			switch (scenarioId) {
			case 0:
				createTbl(*conn, dbId, worker_id);
				break;
			case 1:
				lookup(*conn, dbId, worker_id);
				break;
			case 2:
				append(*conn, dbId, worker_id);
				break;
			default:
				throw std::runtime_error("invalid scenario");
			}

			db_pool.removeWorker(*conn, dbId);
		} catch (...) {
			break;
		}
	}
}

TEST_CASE("Run a concurrent ATTACH/DETACH scenario", "[attach][.]") {
	test_dir_path = TestDirectoryPath();

	DuckDB db(nullptr);
	Connection initConn(db);

	execQuery(initConn, "SET catalog_error_max_schemas = '0'");
	execQuery(initConn, "SET threads = '1'");
	// execQuery(initConn, "SET default_block_size = '16384'");
	// execQuery(initConn, "SET storage_compatibility_version = 'v1.3.2'");

	std::vector<std::thread> workers;
	for (int i = 0; i < worker_count; i++) {
		auto conn = make_uniq<Connection>(db);
		workers.emplace_back(workUnit, std::move(conn), i);
	}

	for (auto &worker : workers) {
		worker.join();
	}
	if (!success) {
		for (auto msg : logging) {
			Printer::Print(msg);
		}
		FAIL();
	}
}
