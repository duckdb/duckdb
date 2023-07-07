//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/preserved_error.hpp"
#include "duckdb/common/arrow/arrow_options.hpp"

namespace duckdb {
struct BoxRendererConfig;

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, STREAM_RESULT, PENDING_RESULT };

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	ClientProperties(string time_zone_p, ArrowOffsetSize arrow_offset_size_p)
	    : time_zone(std::move(time_zone_p)), arrow_offset_size(arrow_offset_size_p) {
	}
	string time_zone;
	ArrowOffsetSize arrow_offset_size;
};

class BaseQueryResult {
public:
	//! Creates a successful query result with the specified names and types
	DUCKDB_API BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
	                           vector<LogicalType> types, vector<string> names);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API BaseQueryResult(QueryResultType type, PreservedError error);
	DUCKDB_API virtual ~BaseQueryResult();

	//! The type of the result (MATERIALIZED or STREAMING)
	QueryResultType type;
	//! The type of the statement that created this result
	StatementType statement_type;
	//! Properties of the statement
	StatementProperties properties;
	//! The SQL types of the result
	vector<LogicalType> types;
	//! The names of the result
	vector<string> names;

public:
	DUCKDB_API void ThrowError(const string &prepended_message = "") const;
	DUCKDB_API void SetError(PreservedError error);
	DUCKDB_API bool HasError() const;
	DUCKDB_API const ExceptionType &GetErrorType() const;
	DUCKDB_API const std::string &GetError();
	DUCKDB_API PreservedError &GetErrorObject();
	DUCKDB_API idx_t ColumnCount();

protected:
	//! Whether or not execution was successful
	bool success;
	//! The error (in case execution was not successful)
	PreservedError error;
};
struct CurrentChunk {
	//! The current data chunk
	unique_ptr<DataChunk> data_chunk;
	//! The current position in the data chunk
	idx_t position;
	//! If we have a current chunk we must scan for result production
	bool Valid();
	//! The remaining size of the current chunk
	idx_t RemainingSize();
};
//! The QueryResult object holds the result of a query. It can either be a MaterializedQueryResult, in which case the
//! result contains the entire result set, or a StreamQueryResult in which case the Fetch method can be called to
//! incrementally fetch data from the database.
class QueryResult : public BaseQueryResult {
public:
	//! Creates a successful query result with the specified names and types
	DUCKDB_API QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
	                       vector<LogicalType> types, vector<string> names, ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API QueryResult(QueryResultType type, PreservedError error);
	DUCKDB_API virtual ~QueryResult() override;

	//! Properties from the client context
	ClientProperties client_properties;
	//! The next result (if any)
	unique_ptr<QueryResult> next;
	//! In case we are converting the result from Native DuckDB to a different library (e.g., Arrow, Polars)
	//! We might be producing chunks of a pre-determined size.
	//! To comply, we use the following variable to store the current chunk, and it's position.
	CurrentChunk current_chunk;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast query result to type - query result type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast query result to type - query result type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}

public:
	//! Returns the name of the column for the given index
	DUCKDB_API const string &ColumnName(idx_t index) const;
	//! Fetches a DataChunk of normalized (flat) vectors from the query result.
	//! Returns nullptr if there are no more results to fetch.
	DUCKDB_API virtual unique_ptr<DataChunk> Fetch();
	//! Fetches a DataChunk from the query result. The vectors are not normalized and hence any vector types can be
	//! returned.
	DUCKDB_API virtual unique_ptr<DataChunk> FetchRaw() = 0;
	//! Converts the QueryResult to a string
	DUCKDB_API virtual string ToString() = 0;
	//! Converts the QueryResult to a box-rendered string
	DUCKDB_API virtual string ToBox(ClientContext &context, const BoxRendererConfig &config);
	//! Prints the QueryResult to the console
	DUCKDB_API void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	DUCKDB_API bool Equals(QueryResult &other);

	bool TryFetch(unique_ptr<DataChunk> &result, PreservedError &error) {
		try {
			result = Fetch();
			return success;
		} catch (const Exception &ex) {
			error = PreservedError(ex);
			return false;
		} catch (std::exception &ex) {
			error = PreservedError(ex);
			return false;
		} catch (...) {
			error = PreservedError("Unknown error in Fetch");
			return false;
		}
	}

	static ArrowOptions GetArrowOptions(QueryResult &query_result);
	static string GetConfigTimezone(QueryResult &query_result);

private:
	class QueryResultIterator;
	class QueryResultRow {
	public:
		explicit QueryResultRow(QueryResultIterator &iterator_p, idx_t row_idx) : iterator(iterator_p), row(0) {
		}

		QueryResultIterator &iterator;
		idx_t row;

		template <class T>
		T GetValue(idx_t col_idx) const {
			return iterator.chunk->GetValue(col_idx, row).GetValue<T>();
		}
	};
	//! The row-based query result iterator. Invoking the
	class QueryResultIterator {
	public:
		explicit QueryResultIterator(optional_ptr<QueryResult> result_p)
		    : current_row(*this, 0), result(result_p), base_row(0) {
			if (result) {
				chunk = shared_ptr<DataChunk>(result->Fetch().release());
				if (!chunk) {
					result = nullptr;
				}
			}
		}

		QueryResultRow current_row;
		shared_ptr<DataChunk> chunk;
		optional_ptr<QueryResult> result;
		idx_t base_row;

	public:
		void Next() {
			if (!chunk) {
				return;
			}
			current_row.row++;
			if (current_row.row >= chunk->size()) {
				base_row += chunk->size();
				chunk = shared_ptr<DataChunk>(result->Fetch().release());
				current_row.row = 0;
				if (!chunk || chunk->size() == 0) {
					// exhausted all rows
					base_row = 0;
					result = nullptr;
					chunk.reset();
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
		const QueryResultRow &operator*() const {
			return current_row;
		}
	};

public:
	QueryResultIterator begin() {
		return QueryResultIterator(this);
	}
	QueryResultIterator end() {
		return QueryResultIterator(nullptr);
	}

protected:
	DUCKDB_API string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb
