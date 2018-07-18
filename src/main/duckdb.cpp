
#include "duckdb.hpp"

#include "parser/parser.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path) {
	// create a database
	// for now we ignore the path and hardcode the lineitem table
	// create the base catalog
	catalog.CreateSchema(DEFAULT_SCHEMA);
	std::vector<ColumnCatalogEntry> columns;
	columns.push_back(ColumnCatalogEntry("l_orderkey", TypeId::INTEGER, true));
	columns.push_back(ColumnCatalogEntry("l_partkey", TypeId::INTEGER, true));
	columns.push_back(ColumnCatalogEntry("l_suppkey", TypeId::INTEGER, true));
	columns.push_back(
	    ColumnCatalogEntry("l_linenumber", TypeId::INTEGER, true));
	columns.push_back(ColumnCatalogEntry("l_quantity", TypeId::INTEGER, true));
	columns.push_back(
	    ColumnCatalogEntry("l_extendedprice", TypeId::DECIMAL, true));
	columns.push_back(ColumnCatalogEntry("l_discount", TypeId::DECIMAL, true));
	columns.push_back(ColumnCatalogEntry("l_tax", TypeId::DECIMAL, true));
	columns.push_back(
	    ColumnCatalogEntry("l_returnflag", TypeId::VARCHAR, true));
	columns.push_back(
	    ColumnCatalogEntry("l_linestatus", TypeId::VARCHAR, true));
	columns.push_back(ColumnCatalogEntry("l_shipdate", TypeId::DATE, true));
	columns.push_back(ColumnCatalogEntry("l_commitdate", TypeId::DATE, true));
	columns.push_back(ColumnCatalogEntry("l_receiptdate", TypeId::DATE, true));
	columns.push_back(
	    ColumnCatalogEntry("l_shipinstruct", TypeId::VARCHAR, true));
	columns.push_back(ColumnCatalogEntry("l_shipmode", TypeId::VARCHAR, true));
	columns.push_back(ColumnCatalogEntry("l_comment", TypeId::VARCHAR, true));

	catalog.CreateTable(DEFAULT_SCHEMA, "lineitem", columns);
}

DuckDBConnection::DuckDBConnection(DuckDB &database) : database(database) {}

DuckDBResult DuckDBConnection::Query(const char *query) {
	// parse the query and transform it into a set of statements
	Parser parser;
	if (!parser.ParseQuery(query)) {
		fprintf(stderr, "Failure to parse: %s\n",
		        parser.GetErrorMessage().c_str());
		return DuckDBResult(parser.GetErrorMessage());
	}

	Planner planner;
	if (!planner.CreatePlan(database.catalog, move(parser.statements.back()))) {
		fprintf(stderr, "Failed to create plan: %s\n",
		        planner.GetErrorMessage().c_str());
		return DuckDBResult(planner.GetErrorMessage());
	}

	// for(auto& statement : parser.statements) {
	// 	// for each of the statements, generate a physical query plan

	// }
	// now transform the parsed query into a physical query plan

	// finally execute the plan and return the result

	return DuckDBResult();
}

DuckDBResult::DuckDBResult() : success(true) {}

DuckDBResult::DuckDBResult(std::string error) : success(false), error(error) {}
