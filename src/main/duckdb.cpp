
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
	// parse the query and transform it into a set of statements
	Parser parser;
	if (!parser.ParseQuery(query.c_str())) {
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
		return make_unique<DuckDBResult>(physical_planner.GetErrorMessage());
	}

	// finally execute the plan and return the result
	Executor executor;
	return move(executor.Execute(move(physical_planner.plan)));
}

DuckDBResult::DuckDBResult() : success(true), count(0) {}

DuckDBResult::DuckDBResult(std::string error)
    : success(false), error(error), count(0) {}

void DuckDBResult::Initialize(DataChunk &chunk) {
	count = 0;
	for (auto i = 0; i < chunk.column_count; i++) {
		types.push_back(chunk.data[i].type);
	}
}

void DuckDBResult::GatherResult(DataChunk &result) {
	result.count = this->count;
	result.Initialize(this->types);
	for (size_t i = 0; i < this->data.size(); i++) {
		result.Append(*this->data[i].get());
	}
}

void DuckDBResult::Print() {
	if (success) {
		for (auto &type : types) {
			printf("%s\t", TypeIdToString(type).c_str());
		}
		printf(" [ %zu ]\n", count);
		for (auto &chunk : data) {
			for (size_t j = 0; j < chunk->count; j++) {
				for (size_t i = 0; i < chunk->column_count; i++) {
					auto &vector = chunk->data[i];
					printf("%s\t", vector.GetValue(j).ToString().c_str());
				}
				printf("\n");
			}
		}
		printf("\n");
	} else {
		printf("Query Error: %s\n", error.c_str());
	}
}
