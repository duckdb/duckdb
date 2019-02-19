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

static void ExecuteStatement_(ClientContext &context, string query, unique_ptr<SQLStatement> statement,
                              DuckDBResult &result) {
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
		result.success = true;
		// we have to log here because some queries are executed in the planner
		if (log_query_string) {
			context.ActiveTransaction().PushQuery(query);
		}

		return;
	}

	auto plan = move(planner.plan);
	Optimizer optimizer(context, *planner.context);
	plan = optimizer.Optimize(move(plan));
	if (!plan) {
		result.success = true;

		return;
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
	result.names = plan->GetNames();

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(context);
	physical_planner.CreatePlan(move(plan));

	// finally execute the plan and return the result
	Executor executor;
	result.collection = executor.Execute(context, physical_planner.plan.get());

	if (log_query_string) {
		context.ActiveTransaction().PushQuery(query);
	}

	result.success = true;
}

unique_ptr<DuckDBResult> DuckDBConnection::GetQueryResult(ClientContext &context, string query) {
	auto result = make_unique<DuckDBResult>();
	result->success = false;

	context.profiler.StartQuery(query);
	context.interrupted = false;
	try {
		// parse the query and transform it into a set of statements
		Parser parser(context);
		parser.ParseQuery(query.c_str());
		if (parser.statements.size() == 0) {
			// empty query
			return make_unique<DuckDBResult>();
		}

		if (parser.statements.size() > 1) {
			throw Exception("More than one statement per query not supported yet!");
		}

		auto &statement = parser.statements.back();
		/*
		#ifdef DEBUG
		        if (statement->type == StatementType::SELECT && context.query_verification_enabled) {
		            // aggressive query verification
		            // copy the statement
		            auto select_stmt = (SelectStatement *)statement.get();
		            auto copied_stmt = select_stmt->Copy();
		            Serializer serializer;
		            select_stmt->Serialize(serializer);
		            Deserializer source(serializer);
		            auto deserialized_stmt = SelectStatement::Deserialize(source);
		            // all the statements should be equal
		            assert(copied_stmt->Equals(statement.get()));
		            assert(deserialized_stmt->Equals(statement.get()));
		            assert(copied_stmt->Equals(deserialized_stmt.get()));
		            DuckDBResult copied_result, deserialized_result;
		            copied_result.success = false;
		            deserialized_result.success = false;

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

		            // execute the original statement
		            try {
		                ExecuteStatement_(context, query, move(statement), *result);
		            } catch (Exception &ex) {
		                result->error = ex.GetMessage();
		            }
		            bool profiling = context.profiler.IsEnabled();
		            if (profiling) {
		                context.profiler.Disable();
		            }
		            // now execute the copied statement
		            try {
		                ExecuteStatement_(context, query, move(copied_stmt), copied_result);
		            } catch (Exception &ex) {
		                copied_result.error = ex.GetMessage();
		            }
		            // now execute the copied statement
		            try {
		                ExecuteStatement_(context, query, move(deserialized_stmt), deserialized_result);
		            } catch (Exception &ex) {
		                deserialized_result.error = ex.GetMessage();
		            }
		            if (profiling) {
		                context.profiler.Enable();
		            }
		            // now compare the results
		            // the results of all three expressions should be identical
		            assert(result->Equals(&copied_result));
		            assert(result->Equals(&deserialized_result));
		            assert(copied_result.Equals(&deserialized_result));
		        } else
		#endif
		*/
		ExecuteStatement_(context, query, move(statement), *result);
	} catch (Exception &ex) {
		result->error = ex.GetMessage();
	} catch (...) {
		result->error = "UNHANDLED EXCEPTION TYPE THROWN!";
	}
	context.profiler.EndQuery();
	if (context.profiler.IsEnabled() && context.profiler.automatic_printing) {
		cout << context.profiler.ToString() << "\n";
	}
	// destroy any data held in the query allocator
	context.allocator.Destroy();
	return result;
}

unique_ptr<DuckDBResult> DuckDBConnection::Query(string query) {
	auto result = make_unique<DuckDBResult>();
	result->success = SendQuery(query);
	result->error = context.execution_context.internal_result.error;
	if (!result->success) {
		return result;
	}
	result->names = context.execution_context.internal_result.names;
	unique_ptr<DataChunk> chunk;
	// FIXME UUGLY
	if (context.execution_context.physical_plan) {
		result->collection.types = context.execution_context.physical_plan->types;
	}
	do {
		chunk = FetchResultChunk();
		result->collection.Append(*chunk.get());
	} while (chunk->size() > 0);

	result->success = CloseResult();
	result->error = context.execution_context.internal_result.error;

	return result;
}

// alternative streaming API
bool DuckDBConnection::SendQuery(string query) {
	CloseResult();

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
		context.execution_context.first_chunk = FetchResultChunk();

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
string DuckDBConnection::GetQueryError() {
	return context.execution_context.internal_result.error;
}
unique_ptr<DataChunk> DuckDBConnection::FetchResultChunk() {
	auto chunk = make_unique<DataChunk>();

	// FIXME UUGLY
	if (context.execution_context.internal_result.success && !context.execution_context.physical_plan) {
		return chunk;
	}
	if (context.execution_context.first_chunk) {
		return move(context.execution_context.first_chunk);
	}
	context.execution_context.physical_plan->InitializeChunk(*chunk.get());
	context.execution_context.physical_plan->GetChunk(context, *chunk.get(),
	                                                  context.execution_context.physical_state.get());
	return chunk;
}

bool DuckDBConnection::CloseResult() {
	context.execution_context.physical_plan = nullptr;

	if (context.transaction.HasActiveTransaction()) {
		context.ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (context.transaction.IsAutoCommit()) {
				if (context.execution_context.internal_result.GetSuccess()) {
					context.transaction.Commit();
				} else {
					context.transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			context.execution_context.internal_result.success = false;
		} catch (...) {
			context.execution_context.internal_result.success = false;
			context.execution_context.internal_result.error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return context.execution_context.internal_result.success;
}
