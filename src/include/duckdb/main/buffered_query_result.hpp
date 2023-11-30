//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/stream_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

class ClientContext;
class ClientContextLock;
class Executor;
class MaterializedQueryResult;
class PreparedStatementData;
class BufferedQueryResult;

class BufferedData {
private:
	static constexpr idx_t BUFFER_SIZE = 100000 / STANDARD_VECTOR_SIZE;

public:
	BufferedData(shared_ptr<ClientContext> context) : context(context) {
	}

public:
	void Populate(unique_ptr<DataChunk> chunk);
	unique_ptr<DataChunk> Fetch(BufferedQueryResult &result);
	void AddToBacklog(InterruptState state);
	void ReplenishBuffer(BufferedQueryResult &result);
	bool BufferIsFull() const;

private:
	shared_ptr<ClientContext> context;
	// Our handles to reschedule the blocked sink tasks
	// TODO: min heap? (priority queue)
	queue<InterruptState> blocked_sinks;
	// Protect against populate/fetch race condition
	mutex glock;
	// Keep track of the size of the buffer to gauge when it should be repopulated
	queue<unique_ptr<DataChunk>> buffered_chunks;
	atomic<idx_t> buffered_chunks_count;
};

class BufferedQueryResult : public QueryResult {
	friend class ClientContext;

public:
	static constexpr const QueryResultType TYPE = QueryResultType::BUFFERED_RESULT;

public:
	//! Create a successful BufferedQueryResult. BufferedQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	DUCKDB_API BufferedQueryResult(StatementType statement_type, StatementProperties properties,
	                               vector<LogicalType> types, vector<string> names, ClientProperties client_properties,
	                               shared_ptr<BufferedData> buffered_data);
	DUCKDB_API ~BufferedQueryResult() override;

public:
	//! Fetches a DataChunk from the query result.
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	DUCKDB_API unique_ptr<MaterializedQueryResult> Materialize();

	DUCKDB_API bool IsOpen();

	//! Closes the BufferedQueryResult
	DUCKDB_API void Close();

	//! The client context this BufferedQueryResult belongs to
	shared_ptr<ClientContext> context;

private:
	unique_ptr<ClientContextLock> LockContext();
	void CheckExecutableInternal(ClientContextLock &lock);
	bool IsOpenInternal(ClientContextLock &lock);

private:
	shared_ptr<BufferedData> buffered_data;
};

} // namespace duckdb
