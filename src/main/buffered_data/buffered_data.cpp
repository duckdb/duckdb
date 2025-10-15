#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

BufferedData::BufferedData(Type type, ClientContext &context_p) : type(type), context(context_p.shared_from_this()) {
	auto &config = ClientConfig::GetConfig(context_p);
	total_buffer_size = config.streaming_buffer_size;
}

BufferedData::~BufferedData() {
}

StreamExecutionResult BufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	auto cc = context.lock();
	if (!cc) {
		return StreamExecutionResult::EXECUTION_CANCELLED;
	}

	StreamExecutionResult execution_result;
	while (!StreamQueryResult::IsChunkReady(execution_result = ExecuteTaskInternal(result, context_lock))) {
		if (execution_result == StreamExecutionResult::BLOCKED) {
			UnblockSinks();
			cc->WaitForTask(context_lock, result);
		}
	}
	if (result.HasError()) {
		Close();
	}
	return execution_result;
}

} // namespace duckdb
