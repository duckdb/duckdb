#include "sqlite3.h"
#include <string>
#include <vector>
#include "util_functions.hpp"

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
	std::vector<std::vector<std::string>> results;

public:
	int Open(std::string filename) {
		return sqlite3_open(filename.c_str(), &db) == SQLITE_OK;
	}

	std::string GetErrorMessage() {
		auto err = sqlite3_errmsg(db);
		return err ? std::string(err) : std::string();
	}

	bool Execute(std::string query) {
		results.clear();
		char *errmsg = nullptr;
		int rc = sqlite3_exec(db, query.c_str(), concatenate_results, &results, &errmsg);
		if (errmsg) {
			sqlite3_free(errmsg);
		}
		return rc == SQLITE_OK;
	}

	void PrintResult() {
		print_result(results);
	}

	bool CheckColumn(size_t column, std::vector<std::string> expected_data) {
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
