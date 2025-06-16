#include "duckdb/main/valid_checker.hpp"

namespace duckdb {

ValidChecker::ValidChecker() : is_invalidated(false) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated(DatabaseInstance &db) {
	if (db.config.options.disable_database_invalidation) {
		return false;
	}
	return is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}

DatabaseInstance &ValidChecker::GetDb(DatabaseInstance &db) {
	return db;
}

DatabaseInstance &ValidChecker::GetDb(MetaTransaction &transaction) {
	return *transaction.context.db;
}

} // namespace duckdb
