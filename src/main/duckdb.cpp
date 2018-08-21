
#include "duckdb.hpp"

#include "common/types/data_chunk.hpp"
#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "optimizer/optimizer.hpp"
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

unique_ptr<DuckDBResult> DuckDBConnection::Query(std::string query) {
	auto result = make_unique<DuckDBResult>();
	result->success = false;
	try {
		// parse the query and transform it into a set of statements
		Parser parser;
		if (!parser.ParseQuery(query.c_str())) {
			fprintf(stderr, "Failure to parse: %s\n",
			        parser.GetErrorMessage().c_str());
			return make_unique<DuckDBResult>(parser.GetErrorMessage());
		}

		Planner planner;
		if (!planner.CreatePlan(database.catalog,
		                        move(parser.statements.back()))) {
			fprintf(stderr, "Failed to create plan: %s\n",
			        planner.GetErrorMessage().c_str());
			return make_unique<DuckDBResult>(planner.GetErrorMessage());
		}
		if (!planner.plan) {
			return make_unique<DuckDBResult>();
		}

		auto plan = move(planner.plan);

		Optimizer optimizer;
		plan = optimizer.Optimize(move(plan));
		if (!optimizer.GetSuccess()) {
			// failed to optimize
			return make_unique<DuckDBResult>(optimizer.GetErrorMessage());
		}
		if (!plan) {
			return make_unique<DuckDBResult>();
		}

		// now convert logical query plan into a physical query plan
		PhysicalPlanGenerator physical_planner(database.catalog);
		if (!physical_planner.CreatePlan(move(plan))) {
			fprintf(stderr, "Failed to create physical plan: %s\n",
			        physical_planner.GetErrorMessage().c_str());
			return make_unique<DuckDBResult>(
			    physical_planner.GetErrorMessage());
		}

		// finally execute the plan and return the result
		Executor executor;
		result->collection = executor.Execute(move(physical_planner.plan));
		result->success = true;
	} catch (Exception ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return move(result);
}

DuckDBResult::DuckDBResult() : success(true) {}

DuckDBResult::DuckDBResult(std::string error) : success(false), error(error) {}

void DuckDBResult::Print() {
	if (success) {
		for (auto &type : types()) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf(" [ %zu ]\n", size());
		for (size_t j = 0; j < size(); j++) {
			for (size_t i = 0; i < column_count(); i++) {
				printf("%s\t", collection.GetValue(i, j).ToString().c_str());
			}
			printf("\n");
		}
		printf("\n");
	} else {
		printf("Query Error: %s\n", error.c_str());
	}
}
