#include "planner/planner.hpp"

#include "common/serializer.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/list.hpp"
#include "planner/binder.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_explain.hpp"

using namespace duckdb;
using namespace std;

void Planner::CreatePlan(ClientContext &context, SQLStatement &statement) {
	// first bind the tables and columns to the catalog
	Binder binder(context);
	binder.Bind(statement);

	// now create a logical query plan from the query
	LogicalPlanGenerator logical_planner(context, *binder.bind_context);
	logical_planner.CreatePlan(statement);

	this->plan = move(logical_planner.root);
	this->context = move(binder.bind_context);
}

void Planner::CreatePlan(ClientContext &context, unique_ptr<SQLStatement> statement) {
	assert(statement);
	switch (statement->type) {
	case StatementType::SELECT:
	case StatementType::INSERT:
	case StatementType::COPY:
	case StatementType::DELETE:
	case StatementType::UPDATE:
	case StatementType::CREATE_INDEX:
	case StatementType::CREATE_TABLE:
		CreatePlan(context, *statement);
		break;
	case StatementType::CREATE_VIEW: {
		auto &stmt = *((CreateViewStatement *)statement.get());

		// plan the view as if it were a query so we can catch errors
		SelectStatement dummy_statement;
		dummy_statement.node = stmt.info->query->Copy();
		CreatePlan(context, (SQLStatement &)dummy_statement);
		if (!plan) {
			throw Exception("Query in view definition contains errors");
		}
		if (stmt.info->aliases.size() > 0) {
			throw NotImplementedException("VIEW aliases");
		}

		context.db.catalog.CreateView(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::CREATE_SCHEMA: {
		auto &stmt = *((CreateSchemaStatement *)statement.get());
		context.db.catalog.CreateSchema(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::DROP_SCHEMA: {
		auto &stmt = *((DropSchemaStatement *)statement.get());
		context.db.catalog.DropSchema(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::DROP_VIEW: {
		auto &stmt = *((DropViewStatement *)statement.get());
		context.db.catalog.DropView(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::DROP_TABLE: {
		auto &stmt = *((DropTableStatement *)statement.get());
		// TODO: create actual plan
		context.db.catalog.DropTable(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::DROP_INDEX: {
		auto &stmt = *((DropIndexStatement *)statement.get());
		// TODO: create actual plan
		context.db.catalog.DropIndex(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::ALTER: {
		// TODO: create actual plan
		auto &stmt = *((AlterTableStatement *)statement.get());
		context.db.catalog.AlterTable(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::TRANSACTION: {
		auto &stmt = *((TransactionStatement *)statement.get());
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
				throw TransactionException("cannot start a transaction within a transaction");
			}
			break;
		}
		case TransactionType::COMMIT: {
			if (context.transaction.IsAutoCommit()) {
				throw TransactionException("cannot commit - no transaction is active");
			} else {
				// explicitly commit the current transaction
				context.transaction.Commit();
				context.transaction.SetAutoCommit(true);
			}
			break;
		}
		case TransactionType::ROLLBACK: {
			if (context.transaction.IsAutoCommit()) {
				throw TransactionException("cannot rollback - no transaction is active");
			} else {
				// explicitly rollback the current transaction
				context.transaction.Rollback();
				context.transaction.SetAutoCommit(true);
			}
			break;
		}
		default:
			throw NotImplementedException("Unrecognized transaction type!");
		}
		break;
	}
	case StatementType::EXPLAIN: {
		auto &stmt = *reinterpret_cast<ExplainStatement *>(statement.get());
		auto parse_tree = stmt.stmt->ToString();
		CreatePlan(context, move(stmt.stmt));
		auto logical_plan_unopt = plan->ToString();
		auto explain = make_unique<LogicalExplain>(move(plan));
		explain->parse_tree = parse_tree;
		explain->logical_plan_unopt = logical_plan_unopt;
		plan = move(explain);
		break;
	}
	default:
		throw NotImplementedException("Statement of type %d not implemented in planner!", statement->type);
	}
}
