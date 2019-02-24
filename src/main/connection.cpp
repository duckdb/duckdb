#include "main/connection.hpp"

#include "main/database.hpp"

using namespace duckdb;
using namespace std;

DuckDBConnection::DuckDBConnection(DuckDB &database) : db(database), context(database) {
	db.connection_manager.AddConnection(this);
}

DuckDBConnection::~DuckDBConnection() {
	context.Cleanup();
	db.connection_manager.RemoveConnection(this);
}
