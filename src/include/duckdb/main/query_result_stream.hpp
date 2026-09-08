//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/query_result_state.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

//! A stream of chunks, opened from the handle of a submitted query. Opening the stream settles the
//! query's retention on draining and consumes the handle: chunks flow through a bounded buffer and
//! are released as the consumer takes them, so there is no retained side and no random access.
class QueryResultStream {
public:
	//! Opens a stream on a submitted query. Throws InvalidInputException when the handle carries an
	//! error, when its retention is already retained, or when the planner marked the statement as
	//! completing before its result is returned. A throw consumes the handle: it is destroyed with
	//! this object, which ends the query
	DUCKDB_API explicit QueryResultStream(unique_ptr<QueryResult> result);
	DUCKDB_API ~QueryResultStream();
	QueryResultStream(const QueryResultStream &) = delete;
	QueryResultStream &operator=(const QueryResultStream &) = delete;

public:
	//! Pops a chunk when one is observable, else reports where execution stands. Runs no task:
	//! chunks are produced by worker threads or by participating calls such as Fetch. After the end
	//! of the stream the terminal state keeps repeating
	DUCKDB_API QueryResultState TryFetch(unique_ptr<DataChunk> &out_chunk);
	//! Runs tasks on the calling thread until a chunk is buffered or the stream ends. Returns null at
	//! the end of the stream, and on an execution error, which is recorded on the stream. After a
	//! clean end it keeps returning null; after an error it throws
	DUCKDB_API unique_ptr<DataChunk> Fetch();

	//! Reports where execution stands. Runs no task
	DUCKDB_API QueryResultState Poll();
	//! Executes a single task of the query on the calling thread. An interrupt or an execution error
	//! is recorded on the stream and reported as ERROR
	DUCKDB_API QueryResultState ExecuteTask();
	//! Blocks until a task is runnable or the engine is waiting on the caller. Runs no task
	DUCKDB_API void WaitForTask();
	//! Ends the stream. Idempotent; the destructor calls it
	DUCKDB_API void Close();
	//! Whether this stream is still the connection's open result
	DUCKDB_API bool IsOpen();

	DUCKDB_API void SetError(ErrorData error);
	DUCKDB_API bool HasError() const;
	DUCKDB_API const string &GetError() const;
	DUCKDB_API const ErrorData &GetErrorObject() const;
	DUCKDB_API const ExceptionType &GetErrorType() const;

	DUCKDB_API const vector<LogicalType> &GetTypes() const;
	DUCKDB_API const vector<Identifier> &GetNames() const;
	DUCKDB_API const Identifier &ColumnName(idx_t index) const;
	DUCKDB_API idx_t ColumnCount() const;
	DUCKDB_API StatementType GetStatementType() const;
	DUCKDB_API const StatementProperties &GetStatementProperties() const;
	DUCKDB_API const ClientProperties &GetClientProperties() const;
	DUCKDB_API ClientProperties &GetClientProperties();

	//! Test hook: the buffer this stream drains
	BufferedData &GetBufferedData() {
		return handle->GetBufferedData();
	}

private:
	unique_ptr<DataChunk> FetchInternal(ClientContextLock &lock);

private:
	//! The handle of the query this stream drains. Never handed out
	unique_ptr<QueryResult> handle;
};

} // namespace duckdb
