#include "main/client_context.hpp"

#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

ClientContext::ClientContext(DuckDB &database)
    : db(database), transaction(database.transaction_manager), interrupted(false),
      prepared_statements(make_unique<CatalogSet>()) {
}

bool ClientContext::Cleanup(int64_t query_number) {
	if (query_number >= 0 && query_number < execution_context.query_number) {
		// result for this query number has already been cleaned up
		return true;
	}
	profiler.EndQuery();
	if (profiler.IsEnabled() && profiler.automatic_printing) {
		cout << profiler.ToString() << "\n";
	}
	// destroy any data held in the query allocator
	allocator.Destroy();

	execution_context.query_number++;
	execution_context.physical_plan = nullptr;

	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (transaction.IsAutoCommit()) {
				if (execution_context.success) {
					transaction.Commit();
				} else {
					transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			execution_context.success = false;
		} catch (...) {
			execution_context.success = false;
			execution_context.error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return execution_context.success;
}

unique_ptr<DataChunk> ClientContext::Fetch(int64_t query_number) {
	auto chunk = make_unique<DataChunk>();
	if (query_number < execution_context.query_number) {
		// result for this query has already been cleaned up, cannot fetch anymore
		return chunk;
	}
	// empty chunk on no-plan queries
	if (execution_context.success && !execution_context.physical_plan) {
		return chunk;
	}
	// if this is the first invocation after Query(), return the chunk it fetched already
	if (execution_context.first_chunk) {
		return move(execution_context.first_chunk);
	}
	// run the plan to get the next chunks
	execution_context.physical_plan->InitializeChunk(*chunk.get());
	execution_context.physical_plan->GetChunk(*this, *chunk.get(), execution_context.physical_state.get());
	return chunk;
}

static void ExecuteStatement_(ClientContext &context, string query, unique_ptr<SQLStatement> statement) {

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

	Planner planner(context);
	planner.CreatePlan(move(statement));
	if (!planner.plan) {
		// we have to log here because some queries are executed in the planner
		if (log_query_string) {
			context.ActiveTransaction().PushQuery(query);
		}
		return;
	}

	auto plan = move(planner.plan);
	// extract the result column names from the plan
	auto names = plan->GetNames();
#ifdef DEBUG
	if (context.enable_optimizer) {
#endif
		Optimizer optimizer(planner.binder, context);
		plan = optimizer.Optimize(move(plan));
		if (!plan) {
			return;
		}

#ifdef DEBUG
	}
#endif

	// special case with logging EXECUTE with prepared statements that do not scan the table
	if (plan->type == LogicalOperatorType::EXECUTE) {
		auto exec = (LogicalExecute *)plan.get();
		if (exec->prep->statement_type == StatementType::UPDATE ||
		    exec->prep->statement_type == StatementType::DELETE) {
			log_query_string = true;
		}
	}

	// extract the result column names from the plan
	context.execution_context.names = names;

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(context);
	physical_planner.CreatePlan(move(plan));

	// store the physical plan in the context for calls to Fetch()
	context.execution_context.physical_plan = move(physical_planner.plan);
	assert(context.execution_context.physical_plan);
	context.execution_context.physical_state = context.execution_context.physical_plan->GetOperatorState();

	// read the first chunk, important for INSERT etc.
	context.execution_context.first_chunk = nullptr;
	context.execution_context.first_chunk = context.Fetch(context.execution_context.query_number);

	if (log_query_string) {
		context.ActiveTransaction().PushQuery(query);
	}
}

unique_ptr<DuckDBStreamingResult> ClientContext::Query(string query) {
	Cleanup();
	int64_t query_number = execution_context.query_number;

	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}

	ActiveTransaction().active_query = db.transaction_manager.GetQueryNumber();

	execution_context.success = true;
	auto result = make_unique<DuckDBStreamingResult>(*this, query_number);

	profiler.StartQuery(query);
	interrupted = false;
	try {
		// parse the query and transform it into a set of statements
		Parser parser(*this);
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() == 0) {
			return result;
		}

		if (parser.statements.size() > 1) {
			throw Exception("More than one statement per query not supported yet!");
		}

		auto &statement = parser.statements.back();
#ifdef DEBUG
		if (statement->type == StatementType::SELECT && query_verification_enabled) {
			// aggressive query verification
			// the purpose of this is to test correctness of otherwise hard to test features:
			// Copy() of statements and expressions
			// Serialize()/Deserialize() of expressions
			// Hash() of expressions
			// Equality() of statements and expressions
			// Correctness of plans both with and without optimizers
			bool profiling_is_enabled = profiler.IsEnabled();
			if (profiling_is_enabled) {
				profiler.Disable();
			}
			// copy the statement
			auto select_stmt = (SelectStatement *)statement.get();
			auto copied_stmt = select_stmt->Copy();
			auto copied_stmt2 = select_stmt->Copy();
			auto copied_stmt3 = select_stmt->Copy();

			Serializer serializer;
			select_stmt->Serialize(serializer);
			Deserializer source(serializer);
			auto deserialized_stmt = SelectStatement::Deserialize(source);
			// all the statements should be equal
			assert(copied_stmt->Equals(statement.get()));
			assert(deserialized_stmt->Equals(statement.get()));
			assert(copied_stmt->Equals(deserialized_stmt.get()));

			// now perform checking on the expressions
			auto &orig_expr_list = select_stmt->node->GetSelectList();
			auto &de_expr_list = ((SelectStatement *)deserialized_stmt.get())->node->GetSelectList();
			auto &cp_expr_list = ((SelectStatement *)copied_stmt.get())->node->GetSelectList();
			assert(orig_expr_list.size() == de_expr_list.size() && cp_expr_list.size() == de_expr_list.size());
			for (size_t i = 0; i < orig_expr_list.size(); i++) {
				// check that the expressions are equivalent
				assert(orig_expr_list[i]->Equals(de_expr_list[i].get()));
				assert(orig_expr_list[i]->Equals(cp_expr_list[i].get()));
				assert(de_expr_list[i]->Equals(cp_expr_list[i].get()));
				// check that the hashes are equivalent too
				assert(orig_expr_list[i]->Hash() == de_expr_list[i]->Hash());
				assert(orig_expr_list[i]->Hash() == cp_expr_list[i]->Hash());
			}
			// now perform additional checking within the expressions
			for (size_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
				auto hash = orig_expr_list[outer_idx]->Hash();
				for (size_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
					auto hash2 = orig_expr_list[inner_idx]->Hash();
					if (hash != hash2) {
						// if the hashes are not equivalent, the expressions should not be equivalent
						assert(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
					}
				}
			}
			unique_ptr<DuckDBResult> materialized_result = make_unique<DuckDBResult>(),
			                         copied_result = make_unique<DuckDBResult>(),
			                         deserialized_result = make_unique<DuckDBResult>(),
			                         unoptimized_result = make_unique<DuckDBResult>();
			// execute the original statement
			try {
				ExecuteStatement_(*this, query, move(statement));
				materialized_result = result->Materialize(false);
			} catch (Exception &ex) {
				materialized_result->error = ex.GetMessage();
			}
			bool profiling = profiler.IsEnabled();
			if (profiling) {
				profiler.Disable();
			}
			// now execute the copied statement
			try {
				ExecuteStatement_(*this, query, move(copied_stmt));
				copied_result = result->Materialize(false);
			} catch (Exception &ex) {
				copied_result->error = ex.GetMessage();
			}
			// now execute the copied statement
			try {
				ExecuteStatement_(*this, query, move(deserialized_stmt));
				deserialized_result = result->Materialize(false);
			} catch (Exception &ex) {
				deserialized_result->error = ex.GetMessage();
			}
			if (profiling) {
				profiler.Enable();
			}
			// now execute the unoptimized statement
			enable_optimizer = false;
			try {
				ExecuteStatement_(*this, query, move(copied_stmt3));
				unoptimized_result = result->Materialize(false);
			} catch (Exception &ex) {
				unoptimized_result->error = ex.GetMessage();
			}
			enable_optimizer = true;

			// now compare the results
			// the results of all three expressions should be identical
			assert(materialized_result->Equals(copied_result.get()));
			assert(materialized_result->Equals(deserialized_result.get()));
			assert(copied_result->Equals(deserialized_result.get()));
			assert(copied_result->Equals(unoptimized_result.get()));

			if (profiling_is_enabled) {
				profiler.Enable();
			}
			// we need to run the query again to make it fetch-able by callers
			ExecuteStatement_(*this, query, move(copied_stmt2));
		} else
#endif
			ExecuteStatement_(*this, query, move(statement));
	} catch (Exception &ex) {
		execution_context.success = false;
		execution_context.error = ex.GetMessage();
	} catch (...) {
		execution_context.success = false;
		execution_context.error = "UNHANDLED EXCEPTION TYPE THROWN!";
	}

	return result;
}
