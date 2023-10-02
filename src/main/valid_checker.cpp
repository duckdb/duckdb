#include "duckdb/main/valid_checker.hpp"

namespace duckdb {

ValidChecker::ValidChecker() : is_invalidated(false) {
}

void ValidChecker::Invalidate(string error) {
	lock_guard<mutex> l(invalidate_lock);
	this->is_invalidated = true;
	this->invalidated_msg = std::move(error);
}

bool ValidChecker::IsInvalidated() {
	return this->is_invalidated;
}

string ValidChecker::InvalidatedMessage() {
	lock_guard<mutex> l(invalidate_lock);
	return invalidated_msg;
}
} // namespace duckdb
