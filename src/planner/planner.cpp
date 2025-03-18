#include "duckdb/planner/planner.hpp"

#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/attached_database.hpp"

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
	auto parameter_count = statement.named_param_map.size();

	BoundParameterMap bound_parameters(parameter_data);

	// first bind the tables and columns to the catalog
	bool parameters_resolved = true;
	try {
		profiler.StartPhase(MetricsType::PLANNER_BINDING);
		binder->parameters = &bound_parameters;
		auto bound_statement = binder->Bind(statement);
		profiler.EndPhase();

		this->names = bound_statement.names;
		this->types = bound_statement.types;
		this->plan = std::move(bound_statement.plan);

		auto max_tree_depth = ClientConfig::GetConfig(context).max_expression_depth;
		CheckTreeDepth(*plan, max_tree_depth);
	} catch (const std::exception &ex) {
		ErrorData error(ex);
		this->plan = nullptr;
		if (error.Type() == ExceptionType::PARAMETER_NOT_RESOLVED) {
			// parameter types could not be resolved
			this->names = {"unknown"};
			this->types = {LogicalTypeId::UNKNOWN};
			parameters_resolved = false;
		} else if (error.Type() != ExceptionType::INVALID) {
			// different exception type - try operator_extensions
			auto &config = DBConfig::GetConfig(context);
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
		} else {
			throw;
		}
	}
	this->properties = binder->GetStatementProperties();
	this->properties.parameter_count = parameter_count;
	properties.bound_all_parameters = !bound_parameters.rebind && parameters_resolved;

	Planner::VerifyPlan(context, plan, bound_parameters.GetParametersPtr());

	// set up a map of parameter number -> value entries
	for (auto &kv : bound_parameters.GetParameters()) {
		auto &identifier = kv.first;
		auto &param = kv.second;
		// check if the type of the parameter could be resolved
		if (!param->return_type.IsValid()) {
			properties.bound_all_parameters = false;
			continue;
		}
		param->SetValue(Value(param->return_type));
		value_map[identifier] = param;
	}
}

shared_ptr<PreparedStatementData> Planner::PrepareSQLStatement(unique_ptr<SQLStatement> statement) {
	auto copied_statement = statement->Copy();
	// create a plan of the underlying statement
	CreatePlan(std::move(statement));
	// now create the logical prepare
	auto prepared_data = make_shared_ptr<PreparedStatementData>(copied_statement->type);
	prepared_data->unbound_statement = std::move(copied_statement);
	prepared_data->names = names;
	prepared_data->types = types;
	prepared_data->value_map = std::move(value_map);
	prepared_data->properties = properties;
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
	case StatementType::SET_STATEMENT:
	case StatementType::LOAD_STATEMENT:
	case StatementType::EXTENSION_STATEMENT:
	case StatementType::PREPARE_STATEMENT:
	case StatementType::EXECUTE_STATEMENT:
	case StatementType::LOGICAL_PLAN_STATEMENT:
	case StatementType::ATTACH_STATEMENT:
	case StatementType::DETACH_STATEMENT:
	case StatementType::COPY_DATABASE_STATEMENT:
	case StatementType::UPDATE_EXTENSIONS_STATEMENT:
		CreatePlan(*statement);
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

static bool OperatorSupportsSerialization(LogicalOperator &op) {
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child)) {
			return false;
		}
	}
	return op.SupportSerialization();
}

void Planner::VerifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                         optional_ptr<bound_parameter_map_t> map) {
	auto &config = DBConfig::GetConfig(context);
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	{
		auto &serialize_comp = config.options.serialization_compatibility;
		auto latest_version = SerializationCompatibility::Latest();
		if (serialize_comp.manually_set &&
		    serialize_comp.serialization_version != latest_version.serialization_version) {
			// Serialization should not be skipped, this test relies on the serialization to remove certain fields for
			// compatibility with older versions. This might change behavior, not doing this might make this test fail.
		} else {
			// if alternate verification is enabled we run the original operator
			return;
		}
	}
#endif
	if (!op || !ClientConfig::GetConfig(context).verify_serializer) {
		return;
	}
	//! SELECT only for now
	if (!OperatorSupportsSerialization(*op)) {
		return;
	}
	// verify the column bindings of the plan
	ColumnBindingResolver::Verify(*op);

	// format (de)serialization of this operator
	try {
		MemoryStream stream(Allocator::Get(context));

		SerializationOptions options;
		if (config.options.serialization_compatibility.manually_set) {
			// Override the default of 'latest' if this was manually set (for testing, mostly)
			options.serialization_compatibility = config.options.serialization_compatibility;
		} else {
			options.serialization_compatibility = SerializationCompatibility::Latest();
		}

		BinarySerializer::Serialize(*op, stream, options);
		stream.Rewind();
		bound_parameter_map_t parameters;
		auto new_plan = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);

		if (map) {
			*map = std::move(parameters);
		}
		op = std::move(new_plan);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		switch (error.Type()) {
		case ExceptionType::NOT_IMPLEMENTED: // NOLINT: explicitly allowing these errors (for now)
			break;                           // pass
		default:
			throw;
		}
	}
}

} // namespace duckdb
