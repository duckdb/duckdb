#include "duckdb/main/buffered_data/file_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

FileBufferedData::FileBufferedData(weak_ptr<ClientContext> context, FileSystem &fs, string file)
  : BufferedData(BufferedData::Type::SIMPLE, std::move(context)) {
  buffered_count = 0;
  file_name = file;
  reader = make_uniq<BufferedFileReader>(fs, file_name.c_str());
}

FileBufferedData::~FileBufferedData() {
}

bool FileBufferedData::BufferIsFull() {
	return buffered_count >= BUFFER_SIZE;
}

PendingExecutionResult FileBufferedData::ReplenishBuffer(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (Closed()) {
		return PendingExecutionResult::EXECUTION_ERROR;
	}
	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return PendingExecutionResult::RESULT_READY;
	}

	
	if (result.HasError()) {
		Close();
	}
}

unique_ptr<DataChunk> FileBufferedData::Scan() {
	if (Closed()) {
		return nullptr;
	}
	lock_guard<mutex> lock(glock);
	if (buffered_chunks.empty()) {
		Close();
		return nullptr;
	}
	auto chunk = std::move(buffered_chunks.front());
	buffered_chunks.pop();

	if (chunk) {
		buffered_count -= chunk->size();
	}
	return chunk;
}

void FileBufferedData::Append(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_count += chunk->size();
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
