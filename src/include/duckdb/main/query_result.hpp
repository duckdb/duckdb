//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/query_result_state.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_scan_states.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_properties.hpp"
#include "duckdb/main/query_result_notifier.hpp"

namespace duckdb {
class BoxRendererContext;
struct BoxRendererConfig;
class BufferedData;
class ClientContext;
class ClientContextLock;
class ColumnDataRowCollection;
class PreparedStatementData;

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, ARROW_RESULT };

class BaseQueryResult {
public:
	//! Creates a successful query result with the specified names and types
	DUCKDB_API BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
	                           vector<LogicalType> types, vector<Identifier> names);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API BaseQueryResult(QueryResultType type, ErrorData error);
	DUCKDB_API virtual ~BaseQueryResult();

public:
	//! Returns the type of the result (MATERIALIZED or ARROW)
	DUCKDB_API QueryResultType GetResultType() const;
	//! Returns the type of the statement that created this result
	DUCKDB_API StatementType GetStatementType() const;
	//! Returns the properties of the statement that created this result
	DUCKDB_API const StatementProperties &GetStatementProperties() const;
	//! Returns the SQL types of the result
	DUCKDB_API const vector<LogicalType> &GetTypes() const;
	//! Returns the names of the result
	DUCKDB_API const vector<Identifier> &GetNames() const;
	//! Returns the number of columns in the result
	DUCKDB_API idx_t ColumnCount() const;

	[[noreturn]] DUCKDB_API void ThrowError(const string &prepended_message = "") const;
	DUCKDB_API void SetError(ErrorData error);
	DUCKDB_API bool HasError() const;
	DUCKDB_API const ExceptionType &GetErrorType() const;
	DUCKDB_API const std::string &GetError() const;
	DUCKDB_API ErrorData &GetErrorObject();
	DUCKDB_API const ErrorData &GetErrorObject() const;

private:
	//! The type of the result (MATERIALIZED or ARROW). Will be removed.
	QueryResultType type;
	//! The type of the statement that created this result
	StatementType statement_type;
	//! Properties of the statement
	StatementProperties properties;
	//! The SQL types of the result
	vector<LogicalType> types;
	//! The names of the result
	vector<Identifier> names;
	//! Whether or not execution was successful
	bool success;
	//! The error (in case execution was not successful)
	ErrorData error;
};

//! A query result. Calling Materialize, Collection, TakeCollection, Fetch, RowCount, and GetValue will materialize the
//! result's data into a ColumnDataCollection. If instead the caller wants a streaming interface, it can be moved into
//! a QueryResultStream.
class QueryResult : public BaseQueryResult {
	friend class BufferedData;
	friend class ClientContext;
	friend class QueryResultStream;

public:
	//! Creates the handle of a freshly submitted query
	DUCKDB_API QueryResult(shared_ptr<ClientContext> context, PreparedStatementData &statement,
	                       vector<LogicalType> types, ClientProperties client_properties,
	                       shared_ptr<BufferedData> buffer);
	//! Creates a detached result over an existing collection
	DUCKDB_API QueryResult(StatementType statement_type, StatementProperties properties, vector<Identifier> names,
	                       unique_ptr<ColumnDataCollection> collection, ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit QueryResult(ErrorData error);
	//! Creates a successful query result of a subclass with the specified names and types
	DUCKDB_API QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
	                       vector<LogicalType> types, vector<Identifier> names, ClientProperties client_properties);
	//! Creates an unsuccessful query result of a subclass
	DUCKDB_API QueryResult(QueryResultType type, ErrorData error);
	DUCKDB_API ~QueryResult() override;

	//! Properties from the client context
	ClientProperties client_properties;
	//! The next result (if any)
	unique_ptr<QueryResult> next;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (GetResultType() != TARGET::TYPE) {
			throw InternalException("Failed to cast query result to type - query result type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (GetResultType() != TARGET::TYPE) {
			throw InternalException("Failed to cast query result to type - query result type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	//! Deduplicate column names for interop with external libraries
	static void DeduplicateColumns(vector<Identifier> &names);
	static void DeduplicateColumns(vector<string> &names);

public:
	//! Returns the query's current state. Does not participate in execution.
	DUCKDB_API QueryResultState Poll();
	//! Executes a single task of the query on the calling thread. Decides nothing: READY means the engine is
	//! waiting for the retention decision, and every further call returns READY, running nothing, until a
	//! stream is opened or a retained-side call (Materialize, Complete, Collection, ...) is made.
	DUCKDB_API QueryResultState ExecuteTask();
	//! Blocks until a task is runnable or the engine is waiting on the caller. Runs no task.
	DUCKDB_API void WaitForTask();
	//! Non-blocking. Tells the engine to fully materialize the result into a CDC. Call Collection(), Fetch[Raw](), or
	//! ExecuteTask() to execute tasks, or (if multithreaded) either Poll or wait on a notification.
	DUCKDB_API void Materialize();
	//! Blocking. Tells the engine to fully materialize the result into a CDC. Participates in execution of the query.
	DUCKDB_API void Complete();
	//! Blocking. Same as Complete(), but will return a reference to the CDC when done.
	DUCKDB_API ColumnDataCollection &Collection();
	//! Blocking. Same as Collection() but takes ownership of the collection. The QueryResult is empty afterward.
	DUCKDB_API unique_ptr<ColumnDataCollection> TakeCollection();
	//! Gets the value of the field at [ column_idx, row_idx ]. Very slow, scanning the collection is much faster.
	//! Will materialize the full result into a CDC if it hadn't yet.
	DUCKDB_API Value GetValue(idx_t column_idx, idx_t row_idx);
	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}
	//! Get the rowcount of the result. Will materialize the full result into a CDC if it hadn't yet.
	DUCKDB_API idx_t RowCount();
	//! Ends the query if it is still open. Idempotent.
	DUCKDB_API void Close();
	//! Whether this result is still the connection's open result.
	DUCKDB_API bool IsOpen();

	//! Returns the name of the column for the given index
	DUCKDB_API const Identifier &ColumnName(idx_t index) const;
	//! A cursor over the collection: fetches the next chunk of normalized (flat) vectors, or null
	//! at the end. Will materialize the full result into a CDC if it hadn't yet.
	DUCKDB_API unique_ptr<DataChunk> Fetch();
	//! Fetches a DataChunk from the query result. The vectors are not normalized and hence any vector types can be
	//! returned. Will materialize the full result into a CDC if it hadn't yet.
	DUCKDB_API unique_ptr<DataChunk> FetchRaw();
	//! Converts the QueryResult to a string
	DUCKDB_API virtual string ToString();
	//! Converts the QueryResult to a box-rendered string
	DUCKDB_API virtual string ToBox(BoxRendererContext &context, const BoxRendererConfig &config);
	//! Prints the QueryResult to the console
	DUCKDB_API void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	DUCKDB_API bool Equals(QueryResult &other, bool compare_names = true);

	bool TryFetchOrError(unique_ptr<DataChunk> &result, ErrorData &error) {
		try {
			result = Fetch();
			return !HasError();
		} catch (std::exception &ex) {
			error = ErrorData(ex);
			return false;
		} catch (...) {
			error = ErrorData("Unknown error in Fetch");
			return false;
		}
	}

	//! False for a detached result and for an error result, which never had a buffer
	bool HasBufferedData() const {
		return buffer != nullptr;
	}
	//! Test helper: the buffer created for this query.
	BufferedData &GetBufferedData() const {
		D_ASSERT(buffer);
		return *buffer;
	}

protected:
	DUCKDB_API virtual unique_ptr<DataChunk> FetchInternal();

private:
	unique_ptr<ClientContextLock> LockContext();
	void CheckExecutableInternal(ClientContextLock &lock);
	bool IsOpenInternal(ClientContextLock &lock);
	//! Records that this result is no longer the connection's active query, the way an interrupt is
	//! recorded, and reports it as an error state
	QueryResultState Cancelled();
	void CompleteInternal(ClientContextLock &lock);
	void HandleFetchFailure(ClientContextLock &lock, ErrorData error);
	//! Ends the query and records a commit failure on this result without throwing
	void EndQuery(ClientContextLock &lock, bool invalidate_transaction = false);
	[[noreturn]] void ThrowNoCollection() const;

private:
	//! The client context this result belongs to. Null once the query has ended
	shared_ptr<ClientContext> context;
	//! The buffer created for this query at submission. It carries the retention decision and, for
	//! a stream, the chunks (null for a detached or an error result)
	shared_ptr<BufferedData> buffer;
	//! Fired when this result's observable state may have changed (may be null)
	shared_ptr<QueryResultNotifier> notifier;
	//! The retained storage (may be null)
	unique_ptr<ColumnDataCollection> collection;
	//! Row collection, only created if GetValue is called
	unique_ptr<ColumnDataRowCollection> row_collection;
	//! Scan state for Fetch calls
	ColumnDataScanState scan_state;
	bool scan_initialized = false;

private:
	class QueryResultIterator;
	class QueryResultRow {
		friend class QueryResultIterator;

	public:
		explicit QueryResultRow() : row(0) {
		}

		bool IsNull(idx_t col_idx) const {
			return chunk->GetValue(col_idx, row).IsNull();
		}
		template <class T>
		T GetValue(idx_t col_idx) const {
			return chunk->GetValue(col_idx, row).GetValue<T>();
		}
		Value GetBaseValue(idx_t col_idx) const {
			return chunk->GetValue(col_idx, row);
		}
		DataChunk &GetChunk() const {
			return *chunk;
		}
		idx_t GetRowInChunk() const {
			return row;
		}

	private:
		shared_ptr<DataChunk> chunk;
		idx_t row;
	};
	//! The row-based query result iterator. Invoking the
	class QueryResultIterator {
	public:
		explicit QueryResultIterator(optional_ptr<QueryResult> result_p = nullptr) : result(result_p), base_row(0) {
			if (result) {
				current_row.chunk = shared_ptr<DataChunk>(result->Fetch().release());
				if (!current_row.chunk) {
					result = nullptr;
				}
			}
		}

		QueryResultRow current_row;
		optional_ptr<QueryResult> result;
		idx_t base_row;

	public:
		void Next() {
			if (!current_row.chunk) {
				return;
			}
			current_row.row++;
			if (current_row.row >= current_row.chunk->size()) {
				base_row += current_row.chunk->size();
				current_row.chunk = shared_ptr<DataChunk>(result->Fetch().release());
				current_row.row = 0;
				if (!current_row.chunk || current_row.chunk->size() == 0) {
					// exhausted all rows
					base_row = 0;
					result = nullptr;
					current_row.chunk.reset();
				}
			}
		}

		QueryResultIterator &operator++() {
			Next();
			return *this;
		}
		bool operator!=(const QueryResultIterator &other) const {
			return result != other.result || base_row != other.base_row || current_row.row != other.current_row.row;
		}
		bool operator==(const QueryResultIterator &other) const {
			return !(*this != other);
		}
		const QueryResultRow &operator*() const {
			return current_row;
		}
	};

public:
	using iterator = QueryResultIterator;

	iterator begin() { // NOLINT: match stl API
		return QueryResultIterator(this);
	}
	iterator end() { // NOLINT: match stl API
		return QueryResultIterator(nullptr);
	}

protected:
	DUCKDB_API string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb
