#include "sqlite3.h"
#include "util_functions.hpp"

class SQLiteStmtWrapper {
public:
	SQLiteStmtWrapper() : stmt(nullptr) {
	}
	~SQLiteStmtWrapper() {
		Finalize();
	}

	sqlite3_stmt *stmt;
	std::string error_message;

	int Prepare(sqlite3 *db, const char *zSql, int nByte, const char **pzTail) {
		Finalize();
		return sqlite3_prepare_v2(db, zSql, nByte, &stmt, pzTail);
	}

	/**
	 * Execute a prepated statment previously "groomed" and print the result
	 */
	int ExecutePreparedStmt() {
		if (!stmt) {
			fprintf(stderr, "There is no a prepared statement: Prepare(...) must be invoked firstly.\n");
			return SQLITE_MISUSE;
		}

		int rc = SQLITE_ROW;
		std::vector<std::vector<std::string>> results; /* To print the result */

		size_t nCol = sqlite3_column_count(stmt);
		char **azCols = (char **)malloc(nCol * sizeof(const char *)); /* Names of result columns */
		char **azVals = (char **)malloc(nCol * sizeof(const char *)); /* Result values */
		if (!azCols || !azVals) {
			rc = SQLITE_NOMEM;
		}
		for (duckdb::idx_t i = 0; i < nCol; i++) {
			azCols[i] = (char *)sqlite3_column_name(stmt, i);
		}

		while (rc == SQLITE_ROW) {
			rc = sqlite3_step(stmt);
			if (rc == SQLITE_ROW) {
				for (duckdb::idx_t i = 0; i < nCol; i++) {
					azVals[i] = (char *)sqlite3_column_text(stmt, i);
					if (!azVals[i] && sqlite3_column_type(stmt, i) != SQLITE_NULL) {
						rc = SQLITE_NOMEM;
						fprintf(stderr, "sqlite3_exec: out of memory.\n");
						break;
					}
				}
				if (concatenate_results(&results, nCol, azVals, azCols)) {
					/* EVIDENCE-OF: R-38229-40159 If the callback function to
					** sqlite3_exec() returns non-zero, then sqlite3_exec() will
					** return SQLITE_ABORT. */
					rc = SQLITE_ABORT;
					fprintf(stderr, "sqlite3_exec: callback returned non-zero. "
					                "Aborting.\n");
					break;
				}
			}
		}

		if (rc == SQLITE_DONE) {
			print_result(results);
		}
		Finalize();
		sqlite3_free(azCols);
		sqlite3_free(azVals);
		return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
	}

	void Finalize() {
		if (stmt) {
			sqlite3_finalize(stmt);
			stmt = nullptr;
		}
	}
};
