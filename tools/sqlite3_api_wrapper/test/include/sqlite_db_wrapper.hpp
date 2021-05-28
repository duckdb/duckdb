#include "sqlite3.h"
#include <string>
#include <vector>

using namespace std;

static int concatenate_results(void *arg, int ncols, char **vals, char **colnames) {
	auto &results = *((vector<vector<string>> *)arg);
	if (results.size() == 0) {
		results.resize(ncols);
	}
	for (int i = 0; i < ncols; i++) {
		results[i].push_back(vals[i] ? vals[i] : "");
	}
	return SQLITE_OK;
}

// C++ wrapper class for the C wrapper API that wraps our C++ API, because why not
class SQLiteDBWrapper {
public:
	SQLiteDBWrapper() : db(nullptr) {
	}
	~SQLiteDBWrapper() {
		if (db) {
			sqlite3_close(db);
		}
	}

	sqlite3 *db;
	vector<vector<string>> results;

public:
	int Open(string filename) {
		return sqlite3_open(filename.c_str(), &db) == SQLITE_OK;
	}

	string GetErrorMessage() {
		auto err = sqlite3_errmsg(db);
		return err ? string(err) : string();
	}

	bool Execute(string query) {
		results.clear();
		char *errmsg = nullptr;
		int rc = sqlite3_exec(db, query.c_str(), concatenate_results, &results, &errmsg);
		if (errmsg) {
			sqlite3_free(errmsg);
		}
		return rc == SQLITE_OK;
	}

	void PrintResult() {
		for (size_t row_idx = 0; row_idx < results[0].size(); row_idx++) {
			for (size_t col_idx = 0; col_idx < results.size(); col_idx++) {
				printf("%s|", results[col_idx][row_idx].c_str());
			}
			printf("\n");
		}
	}

	bool CheckColumn(size_t column, vector<string> expected_data) {
		if (column >= results.size()) {
			fprintf(stderr, "Column index is out of range!\n");
			PrintResult();
			return false;
		}
		if (results[column].size() != expected_data.size()) {
			fprintf(stderr, "Row counts do not match!\n");
			PrintResult();
			return false;
		}
		for (size_t i = 0; i < expected_data.size(); i++) {
			if (expected_data[i] != results[column][i]) {
				fprintf(stderr, "Value does not match: expected \"%s\" but got \"%s\"\n", expected_data[i].c_str(),
				        results[column][i].c_str());
				return false;
			}
		}
		return true;
	}
};

