#include "duckdb/main/client_config.hpp"

#include "duckdb/common/file_system.hpp"

namespace duckdb {

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
