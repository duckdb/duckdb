//===----------------------------------------------------------------------===//
//                         DuckDB
//
// main/connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "main/client_context.hpp"
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

	//! Queries the database, materializes the result immediately
	unique_ptr<DuckDBResult> Query(string query);
	static unique_ptr<DuckDBResult> Query(ClientContext &context, string query);

	//! Queries the database, allows streaming access to result
	unique_ptr<DuckDBStreamingResult> SendQuery(string query);
	static unique_ptr<DuckDBStreamingResult> SendQuery(ClientContext &context, string query);

	DuckDB &db;
	ClientContext context;
};

} // namespace duckdb
