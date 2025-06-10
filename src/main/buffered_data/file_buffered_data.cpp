#include <limits>
#include "duckdb/main/buffered_data/file_buffered_data.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

FileBufferedData::FileBufferedData(weak_ptr<ClientContext> context, FileSystem &fis, string file)
  :BufferedData(BufferedData::Type::SIMPLE, std::move(context)), fs(fis){
  buffered_count = 0;
  file_name = file;
  handle = fs.OpenFile(file, FileFlags::FILE_FLAGS_READ | FileLockType::READ_LOCK);
  done = false;
}

FileBufferedData::~FileBufferedData() {
}

bool FileBufferedData::BufferIsFull() {
	return buffered_count >= BUFFER_SIZE;
}

StreamExecutionResult FileBufferedData::ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) {
	if (Closed() || done) {
		return StreamExecutionResult::EXECUTION_ERROR;
	}

	if (BufferIsFull()) {
		// The buffer isn't empty yet, just return
		return StreamExecutionResult::CHUNK_READY;
	}

	// max blob size is std::numeric_limits<int32_t>::max().

	uint32_t max_blob_size = 64 * 1024;
	auto to_append = make_uniq<DataChunk>();
	vector<LogicalType> types{LogicalType::BLOB};

	to_append->Initialize(Allocator::DefaultAllocator(), types);
	idx_t count = 0;

	for (idx_t col_idx = 0; col_idx < STANDARD_VECTOR_SIZE; col_idx++) {
		auto buffer = make_unsafe_uniq_array<char>(UnsafeNumericCast<size_t>(max_blob_size));
		int64_t bytes_read = fs.Read(*handle, buffer.get(), max_blob_size);
		auto blob_data = std::string(buffer.get(), (size_t)bytes_read);
		to_append->SetValue(0, count, Value::BLOB_RAW(blob_data));
		count++;
		if (bytes_read < max_blob_size) {
			done = true;
			break;
		}
	}
	to_append->SetCardinality(count);
	Append(std::move(to_append));
	if (result.HasError()) {
		Close();
	}
	return StreamExecutionResult::BLOCKED;
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

void FileBufferedData::UnblockSinks() {
}

void FileBufferedData::Append(unique_ptr<DataChunk> chunk) {
	unique_lock<mutex> lock(glock);
	buffered_count += chunk->size();
	buffered_chunks.push(std::move(chunk));
}

} // namespace duckdb
