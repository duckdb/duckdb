#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

RegisteredStateIterator::RegisteredStateIterator(RegisteredStateManager &manager, iterator_type iterator_p,
                                                 idx_t state_version)
    : manager_ref(manager), iterator(std::move(iterator_p)), state_version(state_version) {
}
RegisteredStateIterator &RegisteredStateIterator::operator++() {
	auto &manager = manager_ref.get();

	lock_guard<mutex> lock(manager.lock);
	if (manager.state_version != state_version) {
		throw InternalException("RegisteredStateIterator has been invalidated during iteration");
	}
	++iterator;
	return *this;
}

bool RegisteredStateIterator::operator!=(const RegisteredStateIterator &other) const {
	return state_version == other.state_version && iterator == other.iterator;
}

RegisteredStateIterator::value_type &RegisteredStateIterator::operator*() const {
	auto &manager = manager_ref.get();
	lock_guard<mutex> lock(manager.lock);
	if (manager.state_version != state_version) {
		throw InternalException("RegisteredStateIterator has been invalidated during iteration");
	}
	return *iterator;
}

RegisteredStateIterator RegisteredStateManager::begin() {
	lock_guard<mutex> l(lock);
	return RegisteredStateIterator(*this, registered_state.begin(), state_version);
}

RegisteredStateIterator RegisteredStateManager::end() {
	lock_guard<mutex> l(lock);
	return RegisteredStateIterator(*this, registered_state.end(), state_version);
}

} // namespace duckdb
