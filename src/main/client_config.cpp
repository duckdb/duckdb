#include "duckdb/main/client_config.hpp"

#include "duckdb/common/file_system.hpp"

namespace duckdb {

bool ClientConfig::AnyVerification() const {
	return query_verification_enabled || verify_external || verify_serializer || verify_fetch_row;
}

void ClientConfig::SetUserVariable(const String &name, Value value) {
	user_variables[name.ToStdString()] = std::move(value);
}

bool ClientConfig::GetUserVariable(const string &name, Value &result) {
	auto entry = user_variables.find(name);
	if (entry == user_variables.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

void ClientConfig::ResetUserVariable(const String &name) {
	user_variables.erase(name.ToStdString());
}

void ClientConfig::SetDefaultStreamingBufferSize() {
	auto memory = FileSystem::GetAvailableMemory();
	auto default_size = ClientConfig().streaming_buffer_size;
	if (!memory.IsValid()) {
		streaming_buffer_size = default_size;
		return;
	}
	streaming_buffer_size = MinValue(memory.GetIndex(), default_size);
}

} // namespace duckdb
