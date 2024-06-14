#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

BufferedData::BufferedData(Type type, weak_ptr<ClientContext> context_p) : type(type), context(std::move(context_p)) {
	auto client_context = context.lock();
	auto &config = ClientConfig::GetConfig(*client_context);
	total_buffer_size = config.streaming_buffer_size;
}

BufferedData::~BufferedData() {
}

} // namespace duckdb
