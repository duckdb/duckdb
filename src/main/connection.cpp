#include "main/connection.hpp"

#include "execution/executor.hpp"
#include "execution/physical_plan_generator.hpp"
#include "main/database.hpp"
#include "optimizer/optimizer.hpp"
#include "parser/parser.hpp"
#include "planner/planner.hpp"

using namespace duckdb;
using namespace std;

DuckDBConnection::DuckDBConnection(DuckDB &database) : db(database), context(database) {
	db.connection_manager.AddConnection(this);
}

DuckDBConnection::~DuckDBConnection() {
	db.connection_manager.RemoveConnection(this);
}

static void ExecuteStatement_(ClientContext &context, unique_ptr<SQLStatement> statement, DuckDBResult &result) {
	Planner planner;
	planner.CreatePlan(context, move(statement));
	if (!planner.plan) {
		result.success = true;
		return;
	}

	auto plan = move(planner.plan);
	Optimizer optimizer(context, *planner.context);
	plan = optimizer.Optimize(move(plan));
	if (!plan) {
		result.success = true;
		return;
	}

	// extract the result column names from the plan
	result.names = plan->GetNames();

	// now convert logical query plan into a physical query plan
	PhysicalPlanGenerator physical_planner(context);
	physical_planner.CreatePlan(move(plan));

	// finally execute the plan and return the result
	Executor executor;
	result.collection = executor.Execute(context, physical_planner.plan.get());
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
		if (statement->type == StatementType::UPDATE || statement->type == StatementType::DELETE ||
		    statement->type == StatementType::ALTER || statement->type == StatementType::CREATE_INDEX) {
			// log query in UNDO buffer so it can be saved in the WAL on commit
			auto &transaction = context.transaction.ActiveTransaction();
			transaction.PushQuery(query);
		}
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
				ExecuteStatement_(context, move(statement), *result);
			} catch (Exception &ex) {
				result->error = ex.GetMessage();
			}
			bool profiling = context.profiler.IsEnabled();
			if (profiling) {
				context.profiler.Disable();
			}
			// now execute the copied statement
			try {
				ExecuteStatement_(context, move(copied_stmt), copied_result);
			} catch (Exception &ex) {
				copied_result.error = ex.GetMessage();
			}
			// now execute the copied statement
			try {
				ExecuteStatement_(context, move(deserialized_stmt), deserialized_result);
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
			ExecuteStatement_(context, move(statement), *result);
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

unique_ptr<DuckDBResult> DuckDBConnection::GetQueryResult(string query) {
	return GetQueryResult(context, query);
}

unique_ptr<DuckDBResult> DuckDBConnection::Query(string query) {
	if (context.transaction.IsAutoCommit()) {
		context.transaction.BeginTransaction();
	}

	context.ActiveTransaction().active_query = context.db.transaction_manager.GetQueryNumber();
	auto result = GetQueryResult(query);

	if (context.transaction.HasActiveTransaction()) {
		context.ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		try {
			if (context.transaction.IsAutoCommit()) {
				if (result->GetSuccess()) {
					context.transaction.Commit();
				} else {
					context.transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			result->success = false;
			result->error = ex.GetMessage();
		} catch (...) {
			result->success = false;
			result->error = "UNHANDLED EXCEPTION TYPE THROWN IN TRANSACTION COMMIT!";
		}
	}
	return result;
}
