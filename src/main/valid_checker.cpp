#include "duckdb/main/valid_checker.hpp"

#include "duckdb/main/database.hpp"

namespace duckdb {

ValidChecker::ValidChecker() : is_invalidated(false) {
}

bool ValidChecker::IsInvalidated(DatabaseInstance &db) {
	if (db.config.options.disable_database_invalidation) {
		return false;
	}
	return Get(db).is_invalidated;
}

bool ValidChecker::IsInvalidated(MetaTransaction &transaction) {
	return IsInvalidated(*transaction.context.db);
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}

} // namespace duckdb
