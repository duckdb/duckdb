#include "sqlite_helpers.hpp"

#include "common/types/date.hpp"

using namespace duckdb;
using namespace std;

namespace sqlite {

bool TransferDatabase(DuckDBConnection &con, sqlite3 *sqlite) {
	char *error;
	// start the SQLite transaction
	if (sqlite3_exec(sqlite, "BEGIN TRANSACTION", nullptr, nullptr, &error) != SQLITE_OK) {
		return false;
	}

	// query the list of tables
	auto table_list = con.Query("SELECT name, sql FROM sqlite_master();");

	for (size_t i = 0; i < table_list->size(); i++) {
		auto name = string(table_list->GetValue<const char *>(0, i));
		auto sql = table_list->GetValue<const char *>(1, i);

		// for each table, first create the table in sqlite
		if (sqlite3_exec(sqlite, sql, nullptr, nullptr, &error) != SQLITE_OK) {
			return false;
		}

		// now transfer the actual data
		// first get the data from DuckDB
		auto result = con.Query("SELECT * FROM " + name);
		// create the prepared statement based on the result
		stringstream prepared;
		prepared << "INSERT INTO " << name << " (";
		for (size_t j = 0; j < result->column_count(); j++) {
			prepared << result->names[j];
			if (j + 1 != result->column_count()) {
				prepared << ",";
			}
		}
		prepared << ") VALUES (";
		for (size_t j = 0; j < result->column_count(); j++) {
			prepared << "?";
			if (j + 1 != result->column_count()) {
				prepared << ",";
			}
		}
		prepared << ");";

		auto insert_statement = prepared.str();
		sqlite3_stmt *stmt;
		if (sqlite3_prepare_v2(sqlite, insert_statement.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
			return false;
		}

		auto &types = result->collection.types;
		for (size_t k = 0; k < result->size(); k++) {
			int rc = SQLITE_ERROR;
			for (size_t j = 0; j < types.size(); j++) {
				size_t bind_index = j + 1;
				if (result->ValueIsNull(j, k)) {
					rc = sqlite3_bind_null(stmt, bind_index);
				} else {
					// bind based on the type
					switch (types[j]) {
					case TypeId::BOOLEAN:
						rc = sqlite3_bind_int(stmt, bind_index, (int)result->GetValue<bool>(j, k));
						break;
					case TypeId::TINYINT:
						rc = sqlite3_bind_int(stmt, bind_index, (int)result->GetValue<int8_t>(j, k));
						break;
					case TypeId::SMALLINT:
						rc = sqlite3_bind_int(stmt, bind_index, (int)result->GetValue<int16_t>(j, k));
						break;
					case TypeId::INTEGER:
						rc = sqlite3_bind_int(stmt, bind_index, (int)result->GetValue<int32_t>(j, k));
						break;
					case TypeId::BIGINT:
						rc = sqlite3_bind_int64(stmt, bind_index, (sqlite3_int64)result->GetValue<int64_t>(j, k));
						break;
					case TypeId::POINTER:
						rc = sqlite3_bind_int64(stmt, bind_index, (sqlite3_int64)result->GetValue<uint64_t>(j, k));
						break;
					case TypeId::DATE: {
						auto date = result->GetValue<date_t>(j, k);
						auto date_str = Date::ToString(date) + " 00:00:00";

						rc = sqlite3_bind_text(stmt, bind_index, date_str.c_str(), -1, SQLITE_TRANSIENT);
						break;
					}
					case TypeId::TIMESTAMP:
						// TODO
						break;
					case TypeId::DECIMAL:
						rc = sqlite3_bind_double(stmt, bind_index, result->GetValue<double>(j, k));
						break;
					case TypeId::VARCHAR:
						rc = sqlite3_bind_text(stmt, bind_index, result->GetValue<const char *>(j, k), -1,
						                       SQLITE_TRANSIENT);
						break;
					default:
						break;
					}
				}
				if (rc != SQLITE_OK) {
					return false;
				}
			}
			rc = sqlite3_step(stmt);
			if (rc != SQLITE_DONE) {
				return false;
			}
			if (sqlite3_reset(stmt) != SQLITE_OK) {
				return false;
			}
		}
		sqlite3_finalize(stmt);
	}
	// commit the SQLite transaction
	if (sqlite3_exec(sqlite, "COMMIT", nullptr, nullptr, &error) != SQLITE_OK) {
		return false;
	}
	return true;
}

bool CompareQuery(DuckDBConnection &con, sqlite3 *sqlite, string query) {
	return true;
}

}; // namespace sqlite
