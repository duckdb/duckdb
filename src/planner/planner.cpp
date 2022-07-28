#include "duckdb/planner/planner.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

Planner::Planner(ClientContext &context) : binder(Binder::CreateBinder(context)), context(context) {
}

static void CheckTreeDepth(const LogicalOperator &op, idx_t max_depth, idx_t depth = 0) {
	if (depth >= max_depth) {
		throw ParserException("Maximum tree depth of %lld exceeded in logical planner", max_depth);
	}
	for (auto &child : op.children) {
		CheckTreeDepth(*child, max_depth, depth + 1);
	}
}

void Planner::CreatePlan(SQLStatement &statement) {
	auto &profiler = QueryProfiler::Get(context);
	auto parameter_count = statement.n_param;

	BoundParameterMap bound_parameters(parameter_data);

	// first bind the tables and columns to the catalog
	bool parameters_resolved = true;
	try {
		profiler.StartPhase("binder");
		binder->parameters = &bound_parameters;
		auto bound_statement = binder->Bind(statement);
		profiler.EndPhase();

		this->names = bound_statement.names;
		this->types = bound_statement.types;
		this->plan = move(bound_statement.plan);

		auto max_tree_depth = ClientConfig::GetConfig(context).max_expression_depth;
		CheckTreeDepth(*plan, max_tree_depth);
	} catch (ParameterNotResolvedException &ex) {
		// parameter types could not be resolved
		this->names = {"unknown"};
		this->types = {LogicalTypeId::UNKNOWN};
		this->plan = nullptr;
		parameters_resolved = false;
	}

	this->properties = binder->properties;
	this->properties.parameter_count = parameter_count;
	properties.bound_all_parameters = parameters_resolved;

	// set up a map of parameter number -> value entries
	for (auto &kv : bound_parameters.parameters) {
		auto parameter_index = kv.first;
		auto &parameter_data = kv.second;
		// check if the type of the parameter could be resolved
		if (!parameter_data->return_type.IsValid()) {
			properties.bound_all_parameters = false;
			continue;
		}
		parameter_data->value = Value(parameter_data->return_type);
		value_map[parameter_index] = parameter_data;
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
	prepared_data->properties = properties;
	prepared_data->catalog_version = Transaction::GetTransaction(context).catalog_version;
	return prepared_data;
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
	case StatementType::EXTENSION_STATEMENT:
	case StatementType::PREPARE_STATEMENT:
	case StatementType::EXECUTE_STATEMENT:
		CreatePlan(*statement);
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

} // namespace duckdb
