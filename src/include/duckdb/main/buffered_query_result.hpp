//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/buffered_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"

namespace duckdb {

class ClientContext;
class ClientContextLock;
class Executor;
class MaterializedQueryResult;
class PreparedStatementData;

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
