//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "main/client_context.hpp"
#include "main/prepared_statement.hpp"
#include "main/result.hpp"

namespace duckdb {

class DuckDB;

//! A connection to a database. This represents a (client) connection that can
//! be used to query the database.
class DuckDBConnection {
public:
	DuckDBConnection(DuckDB &database);
	~DuckDBConnection();

	string GetProfilingInformation() {
		return context.profiler.ToString();
	}

	//! Interrupt execution of the current query
	void Interrupt() {
		context.Interrupt();
	}

	void EnableProfiling() {
		context.profiler.Enable();
	}

	void DisableProfiling() {
		context.profiler.Disable();
	}

	//! Enable aggressive verification/testing of queries, should only be used in testing
	void EnableQueryVerification() {
#ifdef DEBUG
		context.query_verification_enabled = true;
#endif
	}

	static unique_ptr<DuckDBResult> GetQueryResult(ClientContext &context, string query);

	unique_ptr<DuckDBPreparedStatement> PrepareStatement(string query);

	//! Queries the database using the transaction context of this connection
	unique_ptr<DuckDBResult> Query(string query);

	DuckDB &db;
	ClientContext context;

private:
	unique_ptr<DuckDBResult> GetQueryResult(string query);
};

} // namespace duckdb
