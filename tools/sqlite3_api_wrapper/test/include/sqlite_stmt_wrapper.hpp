#include "sqlite3.h"

class SQLiteStmtWrapper {
public:
	SQLiteStmtWrapper() : stmt(nullptr) {
	}
	~SQLiteStmtWrapper() {
		Finalize();
	}

	sqlite3_stmt *stmt;
	string error_message;

	int Prepare(sqlite3 *db, const char *zSql, int nByte, const char **pzTail) {
		Finalize();
		return sqlite3_prepare_v2(db, zSql, nByte, &stmt, pzTail);
	}

	void Finalize() {
		if (stmt) {
			sqlite3_finalize(stmt);
			stmt = nullptr;
		}
	}
};

