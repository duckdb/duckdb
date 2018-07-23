
#include "duckdb.hpp"

#include "execution/executor.hpp"
#include "execution/physicalplangenerator.hpp"
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

unique_ptr<DuckDBResult> DuckDBConnection::Query(const char *query) {
	// parse the query and transform it into a set of statements
	Parser parser;
	if (!parser.ParseQuery(query)) {
		fprintf(stderr, "Failure to parse: %s\n",
		        parser.GetErrorMessage().c_str());
		return make_unique<DuckDBResult>(parser.GetErrorMessage());
	}

	Planner planner;
	if (!planner.CreatePlan(database.catalog, move(parser.statements.back()))) {
		fprintf(stderr, "Failed to create plan: %s\n",
		        planner.GetErrorMessage().c_str());
		return make_unique<DuckDBResult>(planner.GetErrorMessage());
	}

	// FIXME: optimize logical plan

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(database.catalog);
	if (!physical_planner.CreatePlan(move(planner.plan),
	                                 move(planner.context))) {
		fprintf(stderr, "Failed to create physical plan: %s\n",
		        physical_planner.GetErrorMessage().c_str());
		return make_unique<DuckDBResult>(physical_planner.GetErrorMessage());
	}

	// finally execute the plan and return the result
	Executor executor;
	return move(executor.Execute(move(physical_planner.plan)));
}

DuckDBResult::DuckDBResult() : success(true) {}

DuckDBResult::DuckDBResult(std::string error) : success(false), error(error) {}

void DuckDBResult::Print() {
	if (success) {
		for (size_t i = 0; i < data.colcount; i++) {
			auto &vector = data.data[i];
			printf("%s\t", TypeIdToString(vector->type).c_str());
		}
		printf(" [ %d ]\n", (int)data.count);
		for (size_t i = 0; i < data.count; i++) {
			for (size_t i = 0; i < data.colcount; i++) {
				auto &vector = data.data[i];
				printf("%s\t", vector->GetValue(i).ToString().c_str());
			}
			printf("\n");
		}
	} else {
		printf("Query Error: %s\n", error.c_str());
	}
}
