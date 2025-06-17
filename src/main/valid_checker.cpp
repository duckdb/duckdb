#include "duckdb/main/valid_checker.hpp"

#include "duckdb/main/database.hpp"

namespace duckdb {

ValidChecker::ValidChecker(DBConfigOptions &options) : is_invalidated(false), options(options) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	is_invalidated = true;
	invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	if (options.disable_database_invalidation) {
		return false;
	}
	return is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}

} // namespace duckdb
