#include "duckdb/planner/planner.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/statement/list.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_sql_statement.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/planner/statement/bound_select_statement.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"

using namespace duckdb;
using namespace std;

Planner::Planner(ClientContext &context) : binder(context), context(context) {
}

void Planner::CreatePlan(SQLStatement &statement, vector<BoundParameterExpression *> *parameters) {
	// first bind the tables and columns to the catalog
	context.profiler.StartPhase("binder");
	binder.parameters = parameters;
	auto bound_statement = binder.Bind(statement);
	context.profiler.EndPhase();

	VerifyQuery(*bound_statement);

	this->names = bound_statement->GetNames();
	this->sql_types = bound_statement->GetTypes();

	// now create a logical query plan from the query
	context.profiler.StartPhase("logical_planner");
	LogicalPlanGenerator logical_planner(binder, context);
	this->plan = logical_planner.CreatePlan(*bound_statement);
	context.profiler.EndPhase();
}

void Planner::CreatePlan(unique_ptr<SQLStatement> statement) {
	assert(statement);
	switch (statement->type) {
	case StatementType::SELECT:
	case StatementType::INSERT:
	case StatementType::COPY:
	case StatementType::DELETE:
	case StatementType::UPDATE:
	case StatementType::CREATE_INDEX:
	case StatementType::CREATE_TABLE:
	case StatementType::EXECUTE:
		CreatePlan(*statement);
		break;
	case StatementType::CREATE_VIEW: {
		auto &stmt = *((CreateViewStatement *)statement.get());

		// plan the view as if it were a query so we can catch errors
		SelectStatement dummy_statement;
		dummy_statement.node = stmt.info->query->Copy();
		CreatePlan((SQLStatement &)dummy_statement);
		if (!plan) {
			throw Exception("Query in view definition contains errors");
		}

		if (stmt.info->aliases.size() > dummy_statement.node->GetSelectList().size()) {
			throw Exception("More VIEW aliases than columns in query result");
		}

		context.catalog.CreateView(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::CREATE_SCHEMA: {
		auto &stmt = *((CreateSchemaStatement *)statement.get());
		context.catalog.CreateSchema(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::CREATE_SEQUENCE: {
		auto &stmt = *((CreateSequenceStatement *)statement.get());
		if (stmt.info->schema == INVALID_SCHEMA) { // no temp sequences
			stmt.info->schema = DEFAULT_SCHEMA;
		}
		context.catalog.CreateSequence(context.ActiveTransaction(), stmt.info.get());
		break;
	}
	case StatementType::DROP: {
		auto &stmt = *((DropStatement *)statement.get());
		switch (stmt.info->type) {
		case CatalogType::SEQUENCE:
			if (stmt.info->schema == INVALID_SCHEMA) { // no temp sequences
				stmt.info->schema = DEFAULT_SCHEMA;
			}
			context.catalog.DropSequence(context.ActiveTransaction(), stmt.info.get());
			break;
		case CatalogType::VIEW:
			if (stmt.info->schema == INVALID_SCHEMA) { // no temp views
				stmt.info->schema = DEFAULT_SCHEMA;
			}
			context.catalog.DropView(context.ActiveTransaction(), stmt.info.get());
			break;
		case CatalogType::TABLE: {
			auto temp = context.temporary_objects->GetTableOrNull(context.ActiveTransaction(), stmt.info->name);
			if (temp && (stmt.info->schema == INVALID_SCHEMA || stmt.info->schema == TEMP_SCHEMA)) {
				context.temporary_objects->DropTable(context.ActiveTransaction(), stmt.info.get());
			} else {
				if (stmt.info->schema == INVALID_SCHEMA) {
					stmt.info->schema = DEFAULT_SCHEMA;
				}
				context.catalog.DropTable(context.ActiveTransaction(), stmt.info.get());
			}
			break;
		}
		case CatalogType::INDEX:
			if (stmt.info->schema == INVALID_SCHEMA) { // no temp views
				stmt.info->schema = DEFAULT_SCHEMA;
			}
			context.catalog.DropIndex(context.ActiveTransaction(), stmt.info.get());
			break;
		case CatalogType::SCHEMA:
			context.catalog.DropSchema(context.ActiveTransaction(), stmt.info.get());
			break;
		default:
			throw NotImplementedException("Unimplemented catalog type for drop statement");
		}
		break;
	}
	case StatementType::ALTER: {
		// TODO: create actual plan
		auto &stmt = *((AlterTableStatement *)statement.get());
		context.catalog.AlterTable(context, stmt.info.get());
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
		CreatePlan(move(stmt.stmt));
		auto logical_plan_unopt = plan->ToString();
		auto explain = make_unique<LogicalExplain>(move(plan));
		explain->logical_plan_unopt = logical_plan_unopt;
		names = {"explain_key", "explain_value"};
		sql_types = {SQLType::VARCHAR, SQLType::VARCHAR};
		plan = move(explain);
		break;
	}
	case StatementType::PREPARE: {
		auto &stmt = *reinterpret_cast<PrepareStatement *>(statement.get());
		auto statement_type = stmt.statement->type;
		if (statement_type == StatementType::EXPLAIN) {
			throw NotImplementedException("Cannot explain prepared statements");
		}
		// first create the plan for the to-be-prepared statement
		vector<BoundParameterExpression *> bound_parameters;
		CreatePlan(*stmt.statement, &bound_parameters);
		// set up a map of parameter number -> value entries
		unordered_map<index_t, PreparedValueEntry> value_map;
		for (auto &expr : bound_parameters) {
			// check if the type of the parameter could be resolved
			if (expr->return_type == TypeId::INVALID) {
				throw BinderException("Could not determine type of parameters: try adding explicit type casts");
			}
			auto value = make_unique<Value>(expr->return_type);
			expr->value = value.get();
			// check if the parameter number has been used before
			if (value_map.find(expr->parameter_nr) != value_map.end()) {
				throw BinderException("Duplicate parameter index. Use $1, $2 etc. to differentiate.");
			}
			PreparedValueEntry entry;
			entry.value = move(value);
			entry.target_type = expr->sql_type;
			value_map[expr->parameter_nr] = move(entry);
		}
		auto prepare =
		    make_unique<LogicalPrepare>(stmt.name, statement_type, names, sql_types, move(value_map), move(plan));
		names = {"Success"};
		sql_types = {SQLType(SQLTypeId::BOOLEAN)};
		plan = move(prepare);
		break;
	}
	case StatementType::DEALLOCATE: {
		auto &stmt = *reinterpret_cast<DeallocateStatement *>(statement.get());
		if (!context.prepared_statements->DropEntry(context.ActiveTransaction(), stmt.name, false)) {
			// silently ignore
		}
		break;
	}
	default:
		throw NotImplementedException("Statement of type %d not implemented in planner!", statement->type);
	}
}

void Planner::VerifyQuery(BoundSQLStatement &statement) {
	if (!context.query_verification_enabled) {
		return;
	}
	if (statement.type != StatementType::SELECT) {
		return;
	}
	auto &select = (BoundSelectStatement &)statement;
	VerifyNode(*select.node);
}

void Planner::VerifyNode(BoundQueryNode &node) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		auto &select_node = (BoundSelectNode &)node;
		vector<unique_ptr<Expression>> copies;
		for (auto &expr : select_node.select_list) {
			VerifyExpression(*expr, copies);
		}
		if (select_node.where_clause) {
			VerifyExpression(*select_node.where_clause, copies);
		}
		for (auto &expr : select_node.groups) {
			VerifyExpression(*expr, copies);
		}
		if (select_node.having) {
			VerifyExpression(*select_node.having, copies);
		}
		for (auto &aggr : select_node.aggregates) {
			VerifyExpression(*aggr, copies);
		}
		for (auto &window : select_node.windows) {
			VerifyExpression(*window, copies);
		}

		// double loop to verify that (in)equality of hashes
		for (index_t i = 0; i < copies.size(); i++) {
			auto outer_hash = copies[i]->Hash();
			for (index_t j = 0; j < copies.size(); j++) {
				auto inner_hash = copies[j]->Hash();
				if (outer_hash != inner_hash) {
					// if hashes are not equivalent the expressions should not be equivalent
					assert(!Expression::Equals(copies[i].get(), copies[j].get()));
				}
			}
		}
	} else {
		assert(node.type == QueryNodeType::SET_OPERATION_NODE);
		auto &setop_node = (BoundSetOperationNode &)node;
		VerifyNode(*setop_node.left);
		VerifyNode(*setop_node.right);
	}
}

void Planner::VerifyExpression(Expression &expr, vector<unique_ptr<Expression>> &copies) {
	if (expr.HasSubquery()) {
		// can't copy subqueries
		return;
	}
	// verify that the copy of expressions works
	auto copy = expr.Copy();
	// copy should have identical hash and identical equality function
	assert(copy->Hash() == expr.Hash());
	assert(Expression::Equals(copy.get(), &expr));
	copies.push_back(move(copy));
}
