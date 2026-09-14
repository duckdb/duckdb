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
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/main/client_context_lock.hpp"

namespace duckdb {

class ClientContext;
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
	DUCKDB_API void WaitForTask() DUCKDB_EXCLUDES(GetClientContext().context_lock);
	//! Executes a single task within the final pipeline, returning whether or not a chunk is ready to be fetched
	DUCKDB_API StreamExecutionResult ExecuteTask() DUCKDB_EXCLUDES(GetClientContext().context_lock);
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	//! Materializes the query result and turns it into a materialized query result
	DUCKDB_API unique_ptr<MaterializedQueryResult> Materialize() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	DUCKDB_API bool IsOpen() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	//! The buffer backing this stream result
	BufferedData &GetBufferedData() {
		D_ASSERT(buffered_data);
		return *buffered_data;
	}
	//! False for an error result, which never had a buffer
	bool HasBufferedData() const {
		return buffered_data != nullptr;
	}

	//! Closes the StreamQueryResult
	DUCKDB_API void Close() DUCKDB_EXCLUDES(GetClientContext().context_lock);

	//! The client context this StreamQueryResult belongs to
	shared_ptr<ClientContext> context;

protected:
	DUCKDB_API unique_ptr<DataChunk> FetchInternal() override DUCKDB_EXCLUDES(GetClientContext().context_lock);

private:
	StreamExecutionResult ExecuteTaskInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);
	unique_ptr<DataChunk> FetchNextInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);
	//! The materialize of a stream a fetch already chose: the remainder is copied out under the cap
	unique_ptr<MaterializedQueryResult> MaterializeByDraining();
	ClientContext &GetClientContext() const;
	void CheckExecutableInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);
	bool IsOpenInternal(ClientContextLock &lock) DUCKDB_REQUIRES(lock);

private:
	shared_ptr<BufferedData> buffered_data;
};

} // namespace duckdb
