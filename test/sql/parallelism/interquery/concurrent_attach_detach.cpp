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

const idx_t dbCount = 10;
const idx_t workerCount = 40;
const idx_t iterationCount = 100;

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
	idx_t TableCount = 0;
	std::vector<TableInfo> tables;
};

DBInfo dbInfos[dbCount];

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

DBPoolMgr dbPool;

void createTbl(Connection &conn, idx_t dbId, idx_t workerId) {
	idx_t nr_initial_rows = 10000;

	lock_guard<mutex> lock(dbInfos[dbId].mu);
	auto tblId = dbInfos[dbId].TableCount;
	dbInfos[dbId].tables.emplace_back(TableInfo {nr_initial_rows});
	dbInfos[dbId].TableCount++;

	string query = "CREATE TABLE " + getDBName(dbId) + ".tbl_" + to_string(tblId) +
	               " AS SELECT "
	               "range::BIGINT AS i, "
	               "range::VARCHAR AS s, "
	               "epoch_ms(range) AS ts "
	               "FROM range(" +
	               to_string(nr_initial_rows) + ")";
	addLog("thread: " + to_string(workerId) + "; q: " + query);
	execQuery(conn, query);
}

void lookup(Connection &conn, idx_t dbId, idx_t workerId) {
	unique_lock<mutex> lock(dbInfos[dbId].mu);
	auto maxTblId = dbInfos[dbId].TableCount;
	if (maxTblId == 0) {
		return;
	}
	auto tblId = std::rand() % maxTblId;
	auto expectedMaxVal = dbInfos[dbId].tables[tblId].size - 1;

	// run query
	auto tableName = getDBName(dbId) + ".tbl_" + to_string(tblId);
	string query = "SELECT i, s, ts FROM " + tableName + " WHERE i = (select max(i) from " + tableName + ")";
	addLog("thread: " + to_string(workerId) + "; q: " + query);
	auto result = execQuery(conn, query);
	lock.unlock();

	REQUIRE(CHECK_COLUMN(result, 0, {Value::INTEGER(expectedMaxVal)}));
	REQUIRE(CHECK_COLUMN(result, 1, {to_string(expectedMaxVal)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::TIMESTAMP(timestamp_t {static_cast<int64_t>(expectedMaxVal * 1000)})}));
}

void append(Connection &conn, idx_t dbId, idx_t workerId, idx_t append_num_rows = STANDARD_VECTOR_SIZE) {
	lock_guard<mutex> lock(dbInfos[dbId].mu);
	auto maxTblId = dbInfos[dbId].TableCount;
	if (maxTblId == 0) {
		return;
	}
	auto tblId = std::rand() % maxTblId;
	idx_t current_num_rows = dbInfos[dbId].tables[tblId].size;
	dbInfos[dbId].tables[tblId].size += append_num_rows;

	// set appender
	auto tblStr = "tbl_" + to_string(tblId);
	addLog("thread: " + to_string(workerId) + "; db: " + getDBName(dbId) + "; table: " + tblStr + "; append rows");
	try {
		duckdb::Appender appender(conn, getDBName(dbId), DEFAULT_SCHEMA, tblStr);

		// fill up datachunk
		DataChunk chunk;
		const duckdb::vector<LogicalType> types = {
			LogicalType::BIGINT,
			LogicalType::VARCHAR,
			LogicalType::TIMESTAMP
		};
		chunk.Initialize(*conn.context, types);

		auto &col_int = chunk.data[0];
		auto data_int = FlatVector::GetData<int64_t>(col_int);
		auto &col_varchar = chunk.data[1];
		auto data_varchar = FlatVector::GetData<string_t>(col_varchar);
		auto &col_ts = chunk.data[2];
		auto data_ts = FlatVector::GetData<timestamp_t>(col_ts);

		for (idx_t row_idx = 0; row_idx < append_num_rows; row_idx++) {
			data_int[row_idx] = current_num_rows + row_idx;
			data_varchar[row_idx] = StringVector::AddString(col_varchar, to_string(current_num_rows + row_idx));
			data_ts[row_idx] = timestamp_t {static_cast<int64_t>(1000 * (current_num_rows + row_idx))};
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

void workUnit(std::unique_ptr<Connection> conn, const idx_t &workerId) {
	for (int i = 0; i < iterationCount; i++) {
		if (!success) {
			break;
		}
		try {
			idx_t scenarioId = std::rand() % 3;
			idx_t dbId = std::rand() % dbCount;

			dbPool.addWorker(*conn, dbId);

			switch (scenarioId) {
			case 0:
				createTbl(*conn, dbId, workerId);
				break;
			case 1:
				lookup(*conn, dbId, workerId);
				break;
			case 2:
				append(*conn, dbId, workerId);
				break;
			default:
				throw std::runtime_error("invalid scenario");
			}

			dbPool.removeWorker(*conn, dbId);
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
	for (int i = 0; i < workerCount; i++) {
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
