#include "duckdb/main/valid_checker.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

ValidChecker::ValidChecker(DatabaseInstance &db) : is_invalidated(false), db(db) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	if (!is_invalidated) {
		return false;
	}
	return !Settings::Get<DisableDatabaseInvalidationSetting>(db);
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}

} // namespace duckdb
