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

namespace duckdb {

enum class QueryResultType : uint8_t { MATERIALIZED_RESULT, STREAM_RESULT, PENDING_RESULT };

//! A set of properties from the client context that can be used to interpret the query result
struct ClientProperties {
	string timezone;
};

class BaseQueryResult {
public:
	//! Creates a successful query result with the specified names and types
	DUCKDB_API BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
	                           vector<LogicalType> types, vector<string> names);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API BaseQueryResult(QueryResultType type, string error);
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
	//! Whether or not execution was successful
	bool success;
	//! The error string (in case execution was not successful)
	string error;

public:
	DUCKDB_API bool HasError();
	DUCKDB_API const string &GetError();
	DUCKDB_API idx_t ColumnCount();
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
	DUCKDB_API QueryResult(QueryResultType type, string error);
	DUCKDB_API virtual ~QueryResult() override;

	//! Properties from the client context
	ClientProperties client_properties;
	//! The next result (if any)
	unique_ptr<QueryResult> next;

public:
	//! Fetches a DataChunk of normalized (flat) vectors from the query result.
	//! Returns nullptr if there are no more results to fetch.
	DUCKDB_API virtual unique_ptr<DataChunk> Fetch();
	//! Fetches a DataChunk from the query result. The vectors are not normalized and hence any vector types can be
	//! returned.
	DUCKDB_API virtual unique_ptr<DataChunk> FetchRaw() = 0;
	//! Converts the QueryResult to a string
	DUCKDB_API virtual string ToString() = 0;
	//! Prints the QueryResult to the console
	DUCKDB_API void Print();
	//! Returns true if the two results are identical; false otherwise. Note that this method is destructive; it calls
	//! Fetch() until both results are exhausted. The data in the results will be lost.
	DUCKDB_API bool Equals(QueryResult &other);

	DUCKDB_API bool TryFetch(unique_ptr<DataChunk> &result, string &error) {
		try {
			result = Fetch();
			return success;
		} catch (std::exception &ex) {
			error = ex.what();
			return false;
		} catch (...) {
			error = "Unknown error in Fetch";
			return false;
		}
	}

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
		explicit QueryResultIterator(QueryResult *result) : current_row(*this, 0), result(result), base_row(0) {
			if (result) {
				chunk = shared_ptr<DataChunk>(result->Fetch().release());
			}
		}

		QueryResultRow current_row;
		shared_ptr<DataChunk> chunk;
		QueryResult *result;
		idx_t base_row;

	public:
		void Next() {
			if (!chunk) {
				return;
			}
			current_row.row++;
			if (current_row.row >= chunk->size()) {
				base_row += chunk->size();
				chunk = result->Fetch();
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
	DUCKDB_API QueryResultIterator begin() {
		return QueryResultIterator(this);
	}
	DUCKDB_API QueryResultIterator end() {
		return QueryResultIterator(nullptr);
	}

protected:
	DUCKDB_API string HeaderToString();

private:
	QueryResult(const QueryResult &) = delete;
};

} // namespace duckdb
