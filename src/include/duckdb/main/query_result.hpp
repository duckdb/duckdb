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
#include "duckdb/main/result_format.hpp"
#include "duckdb/main/result_unit.hpp"

namespace duckdb {
class BoxRendererContext;
struct BoxRendererConfig;
class BufferedData;
class ClientContext;
class ClientContextLock;
class PreparedStatementData;
class QueryResult;

//! How an accessor reaches the representation a format declares. The generic form serves every
//! format whose collection is its ordered units; ChunkFormat specializes it onto the CDC
template <class FORMAT>
struct ResultAccess;

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

//! A query result. Calling Materialize, Collection, TakeCollection, Fetch or RowCount will materialize the
//! result's data into the settled format's collection. If instead the caller wants a streaming interface, it can be
//! moved into a QueryResultStream or a FormattedResultStream.
class QueryResult : public BaseQueryResult {
	friend class BufferedData;
	friend class ClientContext;
	friend class ResultStreamBase;
	template <class FORMAT>
	friend struct ResultAccess;

public:
	//! Creates the handle of a freshly submitted query
	DUCKDB_API QueryResult(shared_ptr<ClientContext> context, PreparedStatementData &statement,
	                       vector<LogicalType> types, ClientProperties client_properties,
	                       shared_ptr<BufferedData> buffer, shared_ptr<ResultFormat> format);
	//! Creates a detached result over an existing collection
	DUCKDB_API QueryResult(StatementType statement_type, StatementProperties properties, vector<Identifier> names,
	                       unique_ptr<ColumnDataCollection> collection, ClientProperties client_properties);
	//! Creates a detached result over the units a non-chunk format produced, and that format's state
	DUCKDB_API QueryResult(StatementType statement_type, StatementProperties properties, vector<LogicalType> types,
	                       vector<Identifier> names, unique_ptr<ResultUnitCollection> units,
	                       shared_ptr<ResultFormat> format, shared_ptr<ResultFormatGlobalState> format_state,
	                       ClientProperties client_properties);
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
	//! Sets the format this result is produced in. Only allowed while the result is undecided: nothing
	//! fetched, no stream opened, not born materialized. Throws InvalidInputException afterwards
	DUCKDB_API void SetFormat(shared_ptr<ResultFormat> format);
	//! The format this result is produced in. The chunk format unless SetFormat or the submission chose another
	DUCKDB_API const ResultFormat &Format() const;
	//! Non-blocking. Tells the engine to fully materialize the result. Call Collection(), Fetch[Raw](), or
	//! ExecuteTask() to execute tasks, or (if multithreaded) Poll until the result is complete.
	DUCKDB_API void Materialize();
	//! Blocking. Tells the engine to fully materialize the result. Participates in execution of the query.
	DUCKDB_API void Complete();
	//! Blocking. Same as Complete(), but will return a reference to the settled format's collection when done.
	//! Throws InvalidInputException when FORMAT is not the settled format
	template <class FORMAT = ChunkFormat>
	typename FORMAT::Collection &Collection() {
		return ResultAccess<FORMAT>::Collection(*this);
	}
	//! Blocking. Same as Collection() but takes ownership of the collection. The QueryResult is empty afterward.
	template <class FORMAT = ChunkFormat>
	unique_ptr<typename FORMAT::Collection> TakeCollection() {
		return ResultAccess<FORMAT>::TakeCollection(*this);
	}
	//! The settled format's per-query state. Throws InvalidInputException before the format is settled,
	//! and when FORMAT is not the settled format
	template <class FORMAT>
	const typename FORMAT::GlobalState &FormatState() const {
		return CheckedFormatState(FORMAT::NAME).template Cast<typename FORMAT::GlobalState>();
	}
	//! Get the rowcount of the result. Will materialize the full result if it hadn't yet.
	DUCKDB_API idx_t RowCount();
	//! Ends the query if it is still open. Idempotent.
	DUCKDB_API void Close();
	//! Whether this result is still the connection's open result.
	DUCKDB_API bool IsOpen();

	//! Returns the name of the column for the given index
	DUCKDB_API const Identifier &ColumnName(idx_t index) const;
	//! A cursor over the collection: for chunks the next chunk of normalized (flat) vectors, for any other
	//! format the next unit, or null at the end. Will materialize the full result if it hadn't yet.
	//! Throws InvalidInputException when FORMAT is not the settled format
	template <class FORMAT = ChunkFormat>
	unique_ptr<typename FORMAT::Unit> Fetch() {
		return ResultAccess<FORMAT>::Fetch(*this);
	}
	//! Fetches a DataChunk from the query result. The vectors are not normalized and hence any vector types can be
	//! returned. Will materialize the full result into a CDC if it hadn't yet. Chunk format only
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
	//! Whether the result already holds its rows
	bool IsCollected() const {
		return collection != nullptr || unit_collection != nullptr;
	}
	//! Copy the format the buffer settled onto this result, so the format outlives the query
	void AdoptSettledFormat();
	//! Move a produced result's rows and format state onto this result
	void AdoptCollected(QueryResult &produced);
	//! Materialize, then throw unless the result succeeded, the settled format is the named one, and
	//! the rows are still here
	void PrepareCollected(const char *expected);
	//! Throws unless the format is settled and is the named one
	const ResultFormatGlobalState &CheckedFormatState(const char *expected) const;
	[[noreturn]] void ThrowFormatMismatch(const char *expected) const;
	//! Whether the settled format is the identity
	bool IsChunkFormat() const;

private:
	//! The client context this result belongs to. Null once the query has ended
	shared_ptr<ClientContext> context;
	//! The buffer created for this query at submission. It carries the retention decision and, for
	//! a stream, the units (null for a detached or an error result)
	shared_ptr<BufferedData> buffer;
	//! The format this result is produced in. Never null
	shared_ptr<ResultFormat> format;
	//! The format's per-query state. Null until the format is settled
	shared_ptr<ResultFormatGlobalState> format_state;
	//! The retained storage of a chunk-format result (may be null)
	unique_ptr<ColumnDataCollection> collection;
	//! The retained storage of a result in any other format (may be null)
	unique_ptr<ResultUnitCollection> unit_collection;
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

template <class FORMAT>
struct ResultAccess {
	static_assert(std::is_base_of<ResultUnit, typename FORMAT::Unit>::value,
	              "a format's Unit must derive from ResultUnit");

	static unique_ptr<typename FORMAT::Unit> Fetch(QueryResult &result) {
		result.PrepareCollected(FORMAT::NAME);
		auto unit = result.unit_collection->Fetch();
		if (!unit) {
			return nullptr;
		}
		unit->Cast<typename FORMAT::Unit>();
		return unique_ptr<typename FORMAT::Unit>(static_cast<typename FORMAT::Unit *>(unit.release()));
	}
	static typename FORMAT::Collection &Collection(QueryResult &result) {
		result.PrepareCollected(FORMAT::NAME);
		return *result.unit_collection;
	}
	static unique_ptr<typename FORMAT::Collection> TakeCollection(QueryResult &result) {
		result.PrepareCollected(FORMAT::NAME);
		return std::move(result.unit_collection);
	}
};

template <>
struct ResultAccess<ChunkFormat> {
	static unique_ptr<DataChunk> Fetch(QueryResult &result) {
		auto chunk = result.FetchRaw();
		if (!chunk) {
			return nullptr;
		}
		chunk->Flatten();
		return chunk;
	}
	static ColumnDataCollection &Collection(QueryResult &result) {
		result.PrepareCollected(ChunkFormat::NAME);
		return *result.collection;
	}
	static unique_ptr<ColumnDataCollection> TakeCollection(QueryResult &result) {
		result.PrepareCollected(ChunkFormat::NAME);
		return std::move(result.collection);
	}
};

} // namespace duckdb
