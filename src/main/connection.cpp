#include "main/connection.hpp"

#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

DuckDBConnection::DuckDBConnection(DuckDB &database) : db(database), context(database) {
	db.connection_manager.AddConnection(this);
}

DuckDBConnection::~DuckDBConnection() {
	CloseResult();
	db.connection_manager.RemoveConnection(this);
}

unique_ptr<DuckDBResult> DuckDBConnection::GetQueryResult(ClientContext &context, string query) {
	return Query(context, query);
}

unique_ptr<DuckDBResult> DuckDBConnection::Query(ClientContext &context, string query) {
	auto result = make_unique<DuckDBResult>();
	result->success = SendQuery(context, query);
	result->error = context.execution_context.internal_result.error;
	if (!result->success) {
		return result;
	}
	result->names = context.execution_context.internal_result.names;
	unique_ptr<DataChunk> chunk;
	if (context.execution_context.physical_plan) {
		result->collection.types = context.execution_context.physical_plan->types;
	}
	do {
		chunk = context.FetchChunk();
		result->collection.Append(*chunk.get());
	} while (chunk->size() > 0);

	result->success = context.CleanupLazyResult();
	result->error = context.execution_context.internal_result.error;

	return result;
}

unique_ptr<DuckDBResult> DuckDBConnection::Query(string query) {
	return Query(context, query);
}

bool DuckDBConnection::SendQuery(ClientContext &context, string query) {
	context.CleanupLazyResult();

	if (context.transaction.IsAutoCommit()) {
		context.transaction.BeginTransaction();
	}

	context.ActiveTransaction().active_query = context.db.transaction_manager.GetQueryNumber();

	context.execution_context.internal_result.success = false;

	context.profiler.StartQuery(query);
	context.interrupted = false;
	try {
		// parse the query and transform it into a set of statements
		Parser parser(context);
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() == 0) {
			// empty query
			return true;
		}

		if (parser.statements.size() > 1) {
			throw Exception("More than one statement per query not supported yet!");
		}

		auto &statement = parser.statements.back();

		Planner planner;

		// for many statements, we log the literal query string in the WAL
		// also note the exception for EXECUTE below
		bool log_query_string = false;
		switch (statement->type) {
		case StatementType::UPDATE:
		case StatementType::DELETE:
		case StatementType::ALTER:
		case StatementType::CREATE_INDEX:
		case StatementType::PREPARE:
		case StatementType::DEALLOCATE:
			log_query_string = true;
			break;
		default:
			break;
		}

		planner.CreatePlan(context, move(statement));
		if (!planner.plan) {
			context.execution_context.internal_result.success = true;
			// we have to log here because some queries are executed in the planner
			if (log_query_string) {
				context.ActiveTransaction().PushQuery(query);
			}

			return true;
		}

		auto plan = move(planner.plan);
		Optimizer optimizer(context, *planner.context);
		plan = optimizer.Optimize(move(plan));
		if (!plan) {
			context.execution_context.internal_result.success = true;

			return true;
		}

		// special case with logging EXECUTE with prepared statements that do not scan the table
		if (plan->type == LogicalOperatorType::EXECUTE) {
			auto exec = (LogicalExecute *)plan.get();
			if (exec->prep->statement_type == StatementType::UPDATE ||
			    exec->prep->statement_type == StatementType::DELETE) {
				log_query_string = true;
			}
		}

		// extract the result column names from the plan
		context.execution_context.internal_result.names = plan->GetNames();

		// now convert logical query plan into a physical query plan
		PhysicalPlanGenerator physical_planner(context);
		physical_planner.CreatePlan(move(plan));

		// finally execute the plan and return the result

		context.execution_context.physical_plan = move(physical_planner.plan);

		assert(context.execution_context.physical_plan);
		// the chunk and state are used to iterate over the input plan
		context.execution_context.physical_state = context.execution_context.physical_plan->GetOperatorState(nullptr);

		context.execution_context.first_chunk = nullptr;
		// read the first chunk
		context.execution_context.first_chunk = context.FetchChunk();

		if (log_query_string) {
			context.ActiveTransaction().PushQuery(query);
		}

		context.execution_context.internal_result.success = true;

	} catch (Exception &ex) {
		context.execution_context.internal_result.error = ex.GetMessage();
	} catch (...) {
		context.execution_context.internal_result.error = "UNHANDLED EXCEPTION TYPE THROWN!";
	}
	context.profiler.EndQuery();
	if (context.profiler.IsEnabled() && context.profiler.automatic_printing) {
		cout << context.profiler.ToString() << "\n";
	}
	// destroy any data held in the query allocator
	context.allocator.Destroy();
	return context.execution_context.internal_result.success;
}

// alternative streaming API
bool DuckDBConnection::SendQuery(string query) {
	return SendQuery(context, query);
}
string DuckDBConnection::GetQueryError() {
	return context.execution_context.internal_result.error;
}
unique_ptr<DataChunk> DuckDBConnection::FetchResultChunk() {
	return context.FetchChunk();
}

bool DuckDBConnection::CloseResult() {
	return context.CleanupLazyResult();
}
