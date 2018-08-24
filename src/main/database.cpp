
#include "main/database.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path)
    : storage(), catalog(storage), transaction_manager() {
	// create a database
	// create the base catalog
	auto transaction = transaction_manager.StartTransaction();
	catalog.CreateSchema(*transaction, DEFAULT_SCHEMA);
	transaction_manager.CommitTransaction(transaction);
}
