//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/materialized_query_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class ClientContext;

// Base class for polymorphism
struct Base {
    virtual ~Base() {} // Make sure to define a virtual destructor
};

// Derived class for integers
struct IntData : public Base {
    int value;

    // Constructor taking an integer argument
    explicit IntData(int val) : value(val) {}
};

// Derived class for doubles
struct DoubleData : public Base {
    double value;

    // Constructor taking a double argument
    explicit DoubleData(double val) : value(val) {}
};

// Derived class for strings
struct StringData : public Base {
    string value;

    // Constructor taking a const char* argument
    explicit StringData(string val) : value(val) {}
};


class MaterializedQueryResult : public QueryResult {
public:
	static constexpr const QueryResultType TYPE = QueryResultType::MATERIALIZED_RESULT;

public:
	friend class ClientContext;
	//! Creates a successful query result with the specified names and types
	DUCKDB_API MaterializedQueryResult(StatementType statement_type, StatementProperties properties,
	                                   vector<string> names, unique_ptr<ColumnDataCollection> collection,
	                                   ClientProperties client_properties);
	//! Creates an unsuccessful query result with error condition
	DUCKDB_API explicit MaterializedQueryResult(ErrorData error);

public:
	//! Fetches a DataChunk from the query result.
	//! This will consume the result (i.e. the result can only be scanned once with this function)
	DUCKDB_API unique_ptr<DataChunk> Fetch() override;
	DUCKDB_API unique_ptr<DataChunk> FetchRaw() override;
	//! Converts the QueryResult to a string
	DUCKDB_API string ToString() override;
	DUCKDB_API string ToBox(ClientContext &context, const BoxRendererConfig &config) override;
	DUCKDB_API std::vector<vector<unique_ptr<Base>>> getContents();

	//! Gets the (index) value of the (column index) column.
	//! Note: this is very slow. Scanning over the underlying collection is much faster.
	DUCKDB_API Value GetValue(idx_t column, idx_t index);

	template <class T>
	T GetValue(idx_t column, idx_t index) {
		auto value = GetValue(column, index);
		return (T)value.GetValue<int64_t>();
	}

	DUCKDB_API idx_t RowCount() const;

	//! Returns a reference to the underlying column data collection
	ColumnDataCollection &Collection();

	//! Takes ownership of the collection, 'collection' is null after this operation
	unique_ptr<ColumnDataCollection> TakeCollection();

private:
	unique_ptr<ColumnDataCollection> collection;
	//! Row collection, only created if GetValue is called
	unique_ptr<ColumnDataRowCollection> row_collection;
	//! Scan state for Fetch calls
	ColumnDataScanState scan_state;
	bool scan_initialized;
};

} // namespace duckdb
