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

	statement.Accept(&binder);

	// now create a logical query plan from the query
	LogicalPlanGenerator logical_planner(context, *binder.bind_context);
	statement.Accept(&logical_planner);

	this->plan = move(logical_planner.root);
	this->context = move(binder.bind_context);
}

void Planner::CreatePlan(ClientContext &context, unique_ptr<SQLStatement> statement) {
	assert(statement);
	switch (statement->type) {
	case StatementType::SELECT:
#ifdef DEBUG
	{
		auto select_stmt = (SelectStatement *)statement.get();
		// in debug mode we test the serialization/deserialization and copying of arbitrary SQL statements
		auto copied_statement = select_stmt->Copy();
		Serializer serializer;
		select_stmt->Serialize(serializer);
		Deserializer source(serializer);
		auto new_statement = SelectStatement::Deserialize(source);
		assert(copied_statement->Equals(statement.get()));
		assert(new_statement->Equals(statement.get()));
		assert(copied_statement->Equals(new_statement.get()));
		CreatePlan(context, *new_statement);
		break;
	}
#endif
	case StatementType::INSERT:
	case StatementType::COPY:
	case StatementType::DELETE:
	case StatementType::UPDATE:
	case StatementType::CREATE_INDEX:
	case StatementType::CREATE_TABLE:
		CreatePlan(context, *statement);
		break;
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
