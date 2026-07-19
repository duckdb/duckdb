#include "duckdb/main/client_config.hpp"

#include "duckdb/common/file_system.hpp"

namespace duckdb {

static Identifier StringToIdentifier(const String &name) {
	return Identifier(string(name.data(), name.size()));
}

void ClientConfig::SetUserVariable(const String &name, Value value) {
	user_variables[StringToIdentifier(name)] = std::move(value);
}

bool ClientConfig::GetUserVariable(const Identifier &name, Value &result) {
	auto entry = user_variables.find(name);
	if (entry == user_variables.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

bool ClientConfig::GetUserVariable(const string &name, Value &result) {
	return GetUserVariable(Identifier(name), result);
}

void ClientConfig::ResetUserVariable(const String &name) {
	user_variables.erase(StringToIdentifier(name));
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
