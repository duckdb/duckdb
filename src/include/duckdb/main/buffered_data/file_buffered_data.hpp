//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/simple_buffered_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class StreamQueryResult;
class ClientContextLock;

class FileBufferedData : public BufferedData {
public:
	static constexpr const BufferedData::Type TYPE = BufferedData::Type::SIMPLE;

private:
	//! (roughly) The max amount of tuples we'll keep buffered at a time
	static constexpr idx_t BUFFER_SIZE = 100000;

public:
	explicit FileBufferedData(weak_ptr<ClientContext> context, FileSystem &fs, string file);
	~FileBufferedData() override;

public:
	void Append(unique_ptr<DataChunk> chunk);
	bool BufferIsFull();
	StreamExecutionResult ExecuteTaskInternal(StreamQueryResult &result, ClientContextLock &context_lock) override;
	unique_ptr<DataChunk> Scan() override;
	void UnblockSinks() override;

private:
	//! The queue of chunks
	queue<unique_ptr<DataChunk>> buffered_chunks;
	//! The current capacity of the buffer (tuples)
	atomic<idx_t> buffered_count;
        string file_name;
        FileSystem &fs;
        unique_ptr<FileHandle> handle;
        bool done;
};

} // namespace duckdb
