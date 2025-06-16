#include "duckdb/main/valid_checker.hpp"

#include "duckdb/main/database.hpp"

namespace duckdb {

ValidChecker::ValidChecker() : is_invalidated(false) {
}

bool ValidChecker::IsInvalidated(DatabaseInstance &db) {
	if (db.config.options.disable_database_invalidation) {
		return false;
	}
	auto &valid_checker = Get(db);

	lock_guard<mutex> l(valid_checker.invalidate_lock);
	return valid_checker.is_invalidated;
}

bool ValidChecker::IsInvalidated(MetaTransaction &transaction) {
	auto &valid_checker = Get(transaction);
	return valid_checker.IsInvalidated(*transaction.context.db);
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
