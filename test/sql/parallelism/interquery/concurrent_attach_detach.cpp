#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"

#include <string>
#include <thread>

using namespace duckdb;
using namespace std;

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
atomic<bool> success;

void execQuery(Connection &conn, const string &query) {
	auto result = conn.Query(query);
	if (result->HasError()) {
		Printer::Print(result->GetError());
		success = false;
	}
}

struct DBInfo {
	mutex mu;
	idx_t lookupTableCount = 0;
	idx_t appendTableCount = 0;
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

void lookup(Connection &conn, idx_t i) {
	unique_lock<mutex> lock(dbInfos[i].mu);
	auto maxTblId = dbInfos[i].lookupTableCount;
	lock.unlock();

	if (maxTblId == 0) {
		return;
	}

	auto tblId = std::rand() % maxTblId;
	string query = "SELECT i, s FROM " + getDBName(i) + ".lookup_tbl_" + to_string(tblId) + " WHERE i = 2049";
	execQuery(conn, query);
}

void createLookupTbl(Connection &conn, idx_t i) {
	lock_guard<mutex> lock(dbInfos[i].mu);
	auto tblId = dbInfos[i].lookupTableCount;
	dbInfos[i].lookupTableCount++;

	string query = "CREATE TABLE " + getDBName(i) + ".lookup_tbl_" + to_string(tblId) +
	               " AS SELECT range AS i, range::VARCHAR AS s FROM range(10000)";
	execQuery(conn, query);
}

void append(Connection &conn, idx_t i) {
	lock_guard<mutex> lock(dbInfos[i].mu);
	auto maxTblId = dbInfos[i].appendTableCount;

	if (maxTblId == 0) {
		return;
	}

	auto tblId = std::rand() % maxTblId;
	auto tblStr = "append_tbl_" + to_string(tblId);
	duckdb::Appender appender(conn, getDBName(i), DEFAULT_SCHEMA, tblStr);
	appender.AppendRow(42, "fourty-two");
	appender.Close();
}

void createAppendTbl(Connection &conn, idx_t i) {
	lock_guard<mutex> lock(dbInfos[i].mu);
	auto tblId = dbInfos[i].appendTableCount;
	dbInfos[i].appendTableCount++;

	string query = "CREATE TABLE " + getDBName(i) + ".append_tbl_" + to_string(tblId) + " (i INTEGER, s VARCHAR)";
	execQuery(conn, query);
}

void workUnit(std::unique_ptr<Connection> conn) {
	for (int i = 0; i < iterationCount; i++) {
		idx_t scenarioId = std::rand() % 4;
		idx_t dbId = std::rand() % dbCount;

		dbPool.addWorker(*conn, dbId);

		switch (scenarioId) {
		case 0:
			lookup(*conn, dbId);
			break;
		case 1:
			createLookupTbl(*conn, dbId);
			break;
		case 2:
			append(*conn, dbId);
			break;
		case 3:
			createAppendTbl(*conn, dbId);
			break;
		default:
			throw runtime_error("invalid scenario");
		}

		dbPool.removeWorker(*conn, dbId);
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

	success = true;
	std::vector<thread> workers;
	for (int i = 0; i < workerCount; i++) {
		auto conn = make_uniq<Connection>(db);
		workers.emplace_back(workUnit, std::move(conn));
	}

	for (auto &worker : workers) {
		worker.join();
	}
	if (!success) {
		FAIL();
	}
}
