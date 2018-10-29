//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// main/connection.hpp
//
// Author: Mark Raasveldt
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

	std::string GetProfilingInformation() {
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

	//! Create an appender that can be used to easily append to the given table.
	//! Note that after creating an appender the connection cannot be used
	//! anymore until the appender is destroyed
	Appender *GetAppender(std::string table_name,
	                      std::string schema = DEFAULT_SCHEMA);
	//! Destroy the current appender, if rollback is true the current
	//! transaction is rolled back, otherwise it is committed
	void DestroyAppender(bool rollback = false);

	static std::unique_ptr<DuckDBResult> GetQueryResult(ClientContext &context,
	                                                    std::string query);

	//! Queries the database using the transaction context of this connection
	std::unique_ptr<DuckDBResult> Query(std::string query);

	DuckDB &db;
	ClientContext context;

  private:
	std::unique_ptr<Appender> appender;
	std::unique_ptr<DuckDBResult> GetQueryResult(std::string query);
};

} // namespace duckdb
