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
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

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
		this->plan = std::move(bound_statement.plan);

		auto max_tree_depth = ClientConfig::GetConfig(context).max_expression_depth;
		CheckTreeDepth(*plan, max_tree_depth);
	} catch (const ParameterNotResolvedException &ex) {
		// parameter types could not be resolved
		this->names = {"unknown"};
		this->types = {LogicalTypeId::UNKNOWN};
		this->plan = nullptr;
		parameters_resolved = false;
	} catch (const Exception &ex) {
		auto &config = DBConfig::GetConfig(context);

		this->plan = nullptr;
		for (auto &extension_op : config.operator_extensions) {
			auto bound_statement =
			    extension_op->Bind(context, *this->binder, extension_op->operator_info.get(), statement);
			if (bound_statement.plan != nullptr) {
				this->names = bound_statement.names;
				this->types = bound_statement.types;
				this->plan = std::move(bound_statement.plan);
				break;
			}
		}

		if (!this->plan) {
			throw;
		}
	} catch (std::exception &ex) {
		throw;
	}
	this->properties = binder->properties;
	this->properties.parameter_count = parameter_count;
	properties.bound_all_parameters = parameters_resolved;

	Planner::VerifyPlan(context, plan, &bound_parameters.parameters);

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
	CreatePlan(std::move(statement));
	// now create the logical prepare
	auto prepared_data = make_shared<PreparedStatementData>(copied_statement->type);
	prepared_data->unbound_statement = std::move(copied_statement);
	prepared_data->names = names;
	prepared_data->types = types;
	prepared_data->value_map = std::move(value_map);
	prepared_data->properties = properties;
	prepared_data->catalog_version = MetaTransaction::Get(context).catalog_version;
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
	case StatementType::LOGICAL_PLAN_STATEMENT:
	case StatementType::ATTACH_STATEMENT:
		CreatePlan(*statement);
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

static bool OperatorSupportsSerialization(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_PREPARE:
	case LogicalOperatorType::LOGICAL_EXECUTE:
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_PRAGMA:
	case LogicalOperatorType::LOGICAL_TRANSACTION:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
	case LogicalOperatorType::LOGICAL_EXPLAIN:
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
	case LogicalOperatorType::LOGICAL_LOAD:
	case LogicalOperatorType::LOGICAL_VACUUM:
		// unsupported (for now)
		return false;
	default:
		break;
	}
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child)) {
			return false;
		}
	}
	return true;
}

void Planner::VerifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &op, bound_parameter_map_t *map) {
	if (!op || !ClientConfig::GetConfig(context).verify_serializer) {
		return;
	}
	//! SELECT only for now
	if (!OperatorSupportsSerialization(*op)) {
		return;
	}

	BufferedSerializer serializer;
	try {
		op->Serialize(serializer);
	} catch (NotImplementedException &ex) {
		// ignore for now (FIXME)
		return;
	}
	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);

	PlanDeserializationState state(context);
	auto new_plan = LogicalOperator::Deserialize(deserializer, state);
	if (map) {
		*map = std::move(state.parameter_data);
	}
	op = std::move(new_plan);
}

} // namespace duckdb
