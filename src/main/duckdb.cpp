
#include "duckdb.hpp"

#include "common/types/data_chunk.hpp"
#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "storage/storage_manager.hpp"

#include "parser/parser.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

DuckDB::DuckDB(const char *path) {
	// create a database
	// create the base catalog
	catalog.CreateSchema(DEFAULT_SCHEMA);
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
	if (!planner.plan) {
		return make_unique<DuckDBResult>();
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
		for (size_t i = 0; i < data.column_count; i++) {
			auto &vector = data.data[i];
			printf("%s\t", TypeIdToString(vector->type).c_str());
		}
		printf(" [ %d ]\n", (int)data.count);
		for (size_t j = 0; j < data.count; j++) {
			for (size_t i = 0; i < data.column_count; i++) {
				auto &vector = data.data[i];
				printf("%s\t", vector->GetValue(j).ToString().c_str());
			}
			printf("\n");
		}
		printf("\n");
	} else {
		printf("Query Error: %s\n", error.c_str());
	}
}
