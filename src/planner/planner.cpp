
#include "parser/statement/copy_statement.hpp"
#include "parser/statement/create_statement.hpp"
#include "parser/statement/insert_statement.hpp"
#include "parser/statement/select_statement.hpp"
#include "parser/statement/transaction_statement.hpp"

#include "planner/binder.hpp"
#include "planner/planner.hpp"

#include "planner/logical_plan_generator.hpp"

using namespace duckdb;
using namespace std;

void Planner::CreatePlan(ClientContext &context, SQLStatement &statement) {
	// first bind the tables and columns to the catalog
	Binder binder(context);

	statement.Accept(&binder);

	// now create a logical query plan from the query
	LogicalPlanGenerator logical_planner(context, *binder.bind_context);
	statement.Accept(&logical_planner);
	// logical_planner.Print();

	this->plan = move(logical_planner.root);
	this->context = move(binder.bind_context);
}

bool Planner::CreatePlan(ClientContext &context,
                         unique_ptr<SQLStatement> statement) {
	this->success = false;
	try {
		switch (statement->GetType()) {
		case StatementType::INSERT:
		case StatementType::COPY:
		case StatementType::SELECT:
			CreatePlan(context, *statement.get());
			this->success = true;
			break;
		case StatementType::CREATE: {
			auto &stmt = *reinterpret_cast<CreateStatement *>(statement.get());
			// TODO: create actual plan
			context.db.catalog.CreateTable(context.ActiveTransaction(),
			                               stmt.schema, stmt.table,
			                               stmt.columns);
			this->success = true;
			break;
		}
		case StatementType::TRANSACTION: {
			auto &stmt =
			    *reinterpret_cast<TransactionStatement *>(statement.get());
			// TODO: create actual plan
			switch (stmt.type) {
			case TransactionType::BEGIN_TRANSACTION: {
				if (context.transaction.IsAutoCommit()) {
					// start the active transaction
					// if autocommit is active, we have already called
					// BeginTransaction by setting autocommit to false we
					// prevent it from being closed after this query, hence
					// preserving the transaction context for the next query
					context.transaction.SetAutoCommit(false);
				} else {
					throw Exception(
					    "cannot start a transaction within a transaction");
				}
				break;
			}
			case TransactionType::COMMIT: {
				if (context.transaction.IsAutoCommit()) {
					throw Exception("cannot commit - no transaction is active");
				} else {
					// explicitly commit the current transaction
					context.transaction.Commit();
					context.transaction.SetAutoCommit(true);
				}
				break;
			}
			case TransactionType::ROLLBACK: {
				if (context.transaction.IsAutoCommit()) {
					throw Exception(
					    "cannot rollback - no transaction is active");
				} else {
					// explicitly rollback the current transaction
					context.transaction.Rollback();
					context.transaction.SetAutoCommit(true);
				}
				break;
			}
			default:
				throw Exception("Unrecognized transaction type!");
			}
			this->success = true;
			break;
		}
		default:
			this->message = StringUtil::Format(
			    "Statement of type %d not implemented!", statement->GetType());
		}
	} catch (Exception ex) {
		this->message = ex.GetMessage();
	} catch (...) {
		this->message = "UNHANDLED EXCEPTION TYPE THROWN IN PLANNER!";
	}
	return this->success;
}
