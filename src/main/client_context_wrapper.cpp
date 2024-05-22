#include "duckdb/main/client_context_wrapper.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ClientContextWrapper::ClientContextWrapper(const shared_ptr<ClientContext> &context) : client_context(context) {
}

shared_ptr<ClientContext> ClientContextWrapper::TryGetContext() {
	auto actual_context = client_context.lock();
	return actual_context;
}

shared_ptr<ClientContext> ClientContextWrapper::GetContext() {
	auto actual_context = TryGetContext();
	if (!actual_context) {
		throw ConnectionException("Connection has already been closed");
	}
	return actual_context;
}

} // namespace duckdb
