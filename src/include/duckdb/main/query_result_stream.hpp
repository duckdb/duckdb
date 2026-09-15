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
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

#include <functional>

namespace duckdb {

//! What every stream shares: the handle of the submitted query, the participation calls, and the pop
//! of a type-erased unit. Opening a stream settles the query's retention on draining and its format,
//! and consumes the handle: units flow through a bounded buffer and are released as the consumer
//! takes them, so there is no retained side and no random access
class ResultStreamBase {
public:
	//! Opens a stream on a submitted query. Throws InvalidInputException when the handle carries an
	//! error, when its retention is already retained, when the planner marked the statement as
	//! completing before its result is returned, or when the settled format is not the expected one.
	//! A throw consumes the handle: it is destroyed with this object, which ends the query
	DUCKDB_API ResultStreamBase(unique_ptr<QueryResult> result, const char *expected_format);
	DUCKDB_API virtual ~ResultStreamBase();
	ResultStreamBase(const ResultStreamBase &) = delete;
	ResultStreamBase &operator=(const ResultStreamBase &) = delete;

public:
	//! Reports READY while a unit is poppable, else where execution stands. Runs no task. An interrupt
	//! or an execution error is recorded on the stream and reported as EXECUTION_ERROR
	DUCKDB_API QueryResultState Poll();
	//! Executes a single task of the query on the calling thread. An interrupt or an execution error
	//! is recorded on the stream and reported as EXECUTION_ERROR
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

protected:
	//! Runs tasks on the calling thread until a unit is buffered or the stream ends. Returns null at
	//! the end of the stream, and on an execution error, which is recorded on the stream
	DUCKDB_API unique_ptr<ResultUnit> FetchUnit();
	//! Pops a unit when one is observable, else reports where execution stands. Runs no task
	DUCKDB_API QueryResultState TryFetchUnit(unique_ptr<ResultUnit> &out_unit);
	//! The settled format's per-query state
	DUCKDB_API const ResultFormatGlobalState &FormatStateInternal() const;

private:
	unique_ptr<ResultUnit> FetchUnitInternal(ClientContextLock &lock);
	//! Runs a buffer call under the context lock and maps any failure onto the stream as
	//! EXECUTION_ERROR. Once the stream has ended it keeps reporting the terminal state
	QueryResultState GuardedInternal(const char *name,
	                                 const std::function<QueryResultState(ClientContextLock &lock)> &call);

private:
	//! The handle of the query this stream drains. Never handed out
	unique_ptr<QueryResult> handle;
};

//! A stream of chunks, opened from the handle of a submitted query. Requires the chunk format
class QueryResultStream : public ResultStreamBase {
public:
	DUCKDB_API explicit QueryResultStream(unique_ptr<QueryResult> result);

public:
	//! Pops a chunk when one is observable, else reports where execution stands. Runs no task:
	//! chunks are produced by worker threads or by participating calls such as Fetch. After the end
	//! of the stream the terminal state keeps repeating
	DUCKDB_API QueryResultState TryFetch(unique_ptr<DataChunk> &out_chunk);
	//! Runs tasks on the calling thread until a chunk is buffered or the stream ends. Returns null at
	//! the end of the stream, and on an execution error, which is recorded on the stream. After a
	//! clean end it keeps returning null; after an error it throws
	DUCKDB_API unique_ptr<DataChunk> Fetch();
};

//! A stream of the units a format produces, opened from the handle of a submitted query. Requires
//! the result's format to be a FORMAT
template <class FORMAT>
class FormattedResultStream : public ResultStreamBase {
public:
	explicit FormattedResultStream(unique_ptr<QueryResult> result) : ResultStreamBase(std::move(result), FORMAT::NAME) {
	}

public:
	unique_ptr<typename FORMAT::Unit> Fetch() {
		return UnitCast(FetchUnit());
	}
	QueryResultState TryFetch(unique_ptr<typename FORMAT::Unit> &out_unit) {
		unique_ptr<ResultUnit> unit;
		auto state = TryFetchUnit(unit);
		out_unit = UnitCast(std::move(unit));
		return state;
	}
	const typename FORMAT::GlobalState &FormatState() const {
		return FormatStateInternal().template Cast<typename FORMAT::GlobalState>();
	}

private:
	static unique_ptr<typename FORMAT::Unit> UnitCast(unique_ptr<ResultUnit> unit) {
		if (!unit) {
			return nullptr;
		}
		unit->Cast<typename FORMAT::Unit>();
		return unique_ptr<typename FORMAT::Unit>(static_cast<typename FORMAT::Unit *>(unit.release()));
	}
};

} // namespace duckdb
