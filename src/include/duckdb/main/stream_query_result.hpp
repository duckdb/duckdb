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
#include "duckdb/common/enums/stream_execution_result.hpp"
#include "duckdb/main/query_result_notifier.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

class ClientContext;
class ClientContextLock;
class Executor;
class MaterializedQueryResult;
class PreparedStatementData;

class StreamQueryResult : public QueryResult {
	friend class ClientContext;

public:
	static constexpr const QueryResultType TYPE = QueryResultType::STREAM_RESULT;

public:
	//! Create a successful StreamQueryResult. StreamQueryResults should always be successful initially (it makes no
	//! sense to stream an error).
	DUCKDB_API StreamQueryResult(StatementType statement_type, StatementProperties properties,
	                             vector<LogicalType> types, vector<Identifier> names,
	                             ClientProperties client_properties, shared_ptr<BufferedData> buffered_data);
	DUCKDB_API explicit StreamQueryResult(ErrorData error);
	DUCKDB_API ~StreamQueryResult() override;

public:
	static bool IsChunkReady(StreamExecutionResult result);
	//! Reschedules the tasks that work on producing a result chunk, returning when at least one task can be executed
	DUCKDB_API void WaitForTask();
	//! Executes a single task within the final pipeline, returning whether or not a chunk is ready to be fetched
	DUCKDB_API StreamExecutionResult ExecuteTask();
	//! Non-blocking fetch for an async result: pops a chunk or reports the stream state; never runs
	//! execution work on the calling thread. CHUNK_READY means out_chunk holds a chunk. After the
	//! stream ends, further calls keep reporting the terminal state.
	DUCKDB_API StreamExecutionResult TryFetchChunk(unique_ptr<DataChunk> &out_chunk);
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	DUCKDB_API unique_ptr<MaterializedQueryResult> Materialize();

	DUCKDB_API bool IsOpen();

	//! Closes the StreamQueryResult
	DUCKDB_API void Close();

	//! The client context this StreamQueryResult belongs to
	shared_ptr<ClientContext> context;

protected:
	DUCKDB_API unique_ptr<DataChunk> FetchInternal() override;

private:
	StreamExecutionResult ExecuteTaskInternal(ClientContextLock &lock);
	unique_ptr<DataChunk> FetchNextInternal(ClientContextLock &lock);
	//! Record a fetch-time failure: process the error, set it on this result, and clean up the query
	void HandleFetchFailure(ClientContextLock &lock, ErrorData error);
	unique_ptr<ClientContextLock> LockContext();
	void CheckExecutableInternal(ClientContextLock &lock);
	bool IsOpenInternal(ClientContextLock &lock);

private:
	shared_ptr<BufferedData> buffered_data;
	//! Notifier shared with the buffered data, the executor, and the client context (may be null)
	shared_ptr<QueryResultNotifier> result_notifier;
};

} // namespace duckdb
