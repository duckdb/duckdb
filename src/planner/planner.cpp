#include "duckdb/planner/planner.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

Planner::Planner(ClientContext &context) : binder(Binder::CreateBinder(context)), context(context) {
}

void Planner::CreatePlan(SQLStatement &statement) {
	auto &profiler = QueryProfiler::Get(context);

	vector<BoundParameterExpression *> bound_parameters;

	// first bind the tables and columns to the catalog
	profiler.StartPhase("binder");
	binder->parameters = &bound_parameters;
	auto bound_statement = binder->Bind(statement);
	profiler.EndPhase();

	this->read_only = binder->read_only;
	this->requires_valid_transaction = binder->requires_valid_transaction;
	this->allow_stream_result = binder->allow_stream_result;
	this->names = bound_statement.names;
	this->types = bound_statement.types;
	this->plan = move(bound_statement.plan);

	// set up a map of parameter number -> value entries
	for (auto &expr : bound_parameters) {
		// check if the type of the parameter could be resolved
		if (expr->return_type.id() == LogicalTypeId::INVALID || expr->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw BinderException("Could not determine type of parameters");
		}
		auto value = make_unique<Value>(expr->return_type);
		expr->value = value.get();
		// check if the parameter number has been used before
		if (value_map.find(expr->parameter_nr) == value_map.end()) {
			// not used before, create vector
			value_map[expr->parameter_nr] = vector<unique_ptr<Value>>();
		} else if (value_map[expr->parameter_nr].back()->type() != value->type()) {
			// used before, but types are inconsistent
			throw BinderException("Inconsistent types found for parameter with index %llu", expr->parameter_nr);
		}
		value_map[expr->parameter_nr].push_back(move(value));
	}
}

shared_ptr<PreparedStatementData> Planner::PrepareSQLStatement(unique_ptr<SQLStatement> statement) {
	auto copied_statement = statement->Copy();
	// create a plan of the underlying statement
	CreatePlan(move(statement));
	// now create the logical prepare
	auto prepared_data = make_shared<PreparedStatementData>(copied_statement->type);
	prepared_data->unbound_statement = move(copied_statement);
	prepared_data->names = names;
	prepared_data->types = types;
	prepared_data->value_map = move(value_map);
	prepared_data->read_only = this->read_only;
	prepared_data->requires_valid_transaction = this->requires_valid_transaction;
	prepared_data->allow_stream_result = this->allow_stream_result;
	prepared_data->catalog_version = Transaction::GetTransaction(context).catalog_version;
	return prepared_data;
}

void Planner::PlanExecute(unique_ptr<SQLStatement> statement) {
	auto &stmt = (ExecuteStatement &)*statement;

	// bind the prepared statement
	auto entry = context.prepared_statements.find(stmt.name);
	if (entry == context.prepared_statements.end()) {
		throw BinderException("Prepared statement \"%s\" does not exist", stmt.name);
	}

	// check if we need to rebind the prepared statement
	// this happens if the catalog changes, since in this case e.g. tables we relied on may have been deleted
	auto prepared = entry->second;
	auto &catalog = Catalog::GetCatalog(context);
	bool rebound = false;
	if (catalog.GetCatalogVersion() != entry->second->catalog_version) {
		// catalog was modified: rebind the statement before running the execute
		prepared = PrepareSQLStatement(entry->second->unbound_statement->Copy());
		if (prepared->types != entry->second->types) {
			throw BinderException("Rebinding statement \"%s\" after catalog change resulted in change of types",
			                      stmt.name);
		}
		rebound = true;
	}

	// the bound prepared statement is ready: bind any supplied parameters
	vector<Value> bind_values;
	for (idx_t i = 0; i < stmt.values.size(); i++) {
		ConstantBinder cbinder(*binder, context, "EXECUTE statement");
		if (prepared->value_map.count(i + 1)) {
			cbinder.target_type = prepared->GetType(i + 1);
		}
		auto bound_expr = cbinder.Bind(stmt.values[i]);

		Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
		bind_values.push_back(move(value));
	}
	prepared->Bind(move(bind_values));
	if (rebound) {
		return;
	}

	// copy the properties of the prepared statement into the planner
	this->read_only = prepared->read_only;
	this->requires_valid_transaction = prepared->requires_valid_transaction;
	this->allow_stream_result = prepared->allow_stream_result;
	this->names = prepared->names;
	this->types = prepared->types;
	this->plan = make_unique<LogicalExecute>(move(prepared));
}

void Planner::PlanPrepare(unique_ptr<SQLStatement> statement) {
	auto &stmt = (PrepareStatement &)*statement;
	auto prepared_data = PrepareSQLStatement(move(stmt.statement));

	auto prepare = make_unique<LogicalPrepare>(stmt.name, move(prepared_data), move(plan));
	// we can prepare in read-only mode: prepared statements are not written to the catalog
	this->read_only = true;
	// we can always prepare, even if the transaction has been invalidated
	// this is required because most clients ALWAYS invoke prepared statements
	this->requires_valid_transaction = false;
	this->allow_stream_result = false;
	this->names = {"Success"};
	this->types = {LogicalType::BOOLEAN};
	this->plan = move(prepare);
}

void Planner::CreatePlan(unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement);
	switch (statement->type) {
	case StatementType::SELECT_STATEMENT:
	case StatementType::INSERT_STATEMENT:
	case StatementType::COPY_STATEMENT:
	case StatementType::DELETE_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::CREATE_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::ALTER_STATEMENT:
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::EXPLAIN_STATEMENT:
	case StatementType::VACUUM_STATEMENT:
	case StatementType::RELATION_STATEMENT:
	case StatementType::CALL_STATEMENT:
	case StatementType::EXPORT_STATEMENT:
	case StatementType::PRAGMA_STATEMENT:
	case StatementType::SHOW_STATEMENT:
	case StatementType::SET_STATEMENT:
	case StatementType::LOAD_STATEMENT:
		CreatePlan(*statement);
		break;
	case StatementType::EXECUTE_STATEMENT:
		PlanExecute(move(statement));
		break;
	case StatementType::PREPARE_STATEMENT:
		PlanPrepare(move(statement));
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

} // namespace duckdb
