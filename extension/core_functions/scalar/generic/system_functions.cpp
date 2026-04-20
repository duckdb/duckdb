#include "duckdb/catalog/catalog_search_path.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace {

// current_query
void CurrentQueryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(state.GetContext().GetCurrentQuery());
	result.Reference(val);
}

// current_schema
// PG-compliant: returns the first schema in the search path that actually
// exists, or NULL if none of them do.
void CurrentSchemaFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &current_catalog = DatabaseManager::GetDefaultDatabase(context);
	// Only user-set entries, with "$user" resolved — no implicit/temp/system.
	auto entries = ClientData::Get(context).catalog_search_path->GetResolvedSetPaths();
	for (auto &entry : entries) {
		if (entry.catalog != current_catalog) {
			continue;
		}
		auto schema_entry =
		    Catalog::GetSchema(context, entry.catalog, entry.schema, OnEntryNotFound::RETURN_NULL);
		if (schema_entry) {
			result.Reference(Value(entry.schema));
			return;
		}
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

// current_database
void CurrentDatabaseFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(DatabaseManager::GetDefaultDatabase(state.GetContext()));
	result.Reference(val);
}

struct CurrentSchemasBindData : public FunctionData {
	explicit CurrentSchemasBindData(Value result_value) : result(std::move(result_value)) {
	}

	Value result;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentSchemasBindData>(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<CurrentSchemasBindData>();
		return Value::NotDistinctFrom(result, other.result);
	}
};

unique_ptr<FunctionData> CurrentSchemasBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments[0]->return_type.id() != LogicalTypeId::BOOLEAN) {
		throw BinderException("current_schemas requires a boolean input");
	}
	if (!arguments[0]->IsFoldable()) {
		throw NotImplementedException("current_schemas requires a constant input");
	}
	Value schema_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	Value result_val;
	if (schema_value.IsNull()) {
		// null
		result_val = Value(LogicalType::LIST(LogicalType::VARCHAR));
	} else {
		auto implicit_schemas = BooleanValue::Get(schema_value);
		vector<Value> schema_list;
		auto &catalog_search_path = ClientData::Get(context).catalog_search_path;
		auto &current_catalog = DatabaseManager::GetDefaultDatabase(context);
		// PG-compliant implicit prefix: pg_temp (if the session has any temp
		// objects), then pg_catalog. Then user-set schemas in the current DB.
		if (implicit_schemas) {
			auto temp_db = DatabaseManager::Get(context).GetDatabase(context, TEMP_CATALOG);
			if (temp_db) {
				// Sentinel exception to abort Scan on the first hit. SchemaCatalogEntry::Scan
				// has no early-exit callback, so throw/catch is the cheapest way to bail out.
				struct FoundTempObject : std::exception {};
				bool has_temp_objects = false;
				try {
					for (auto &schema_ref : temp_db->GetCatalog().GetSchemas(context)) {
						schema_ref.get().Scan(context, CatalogType::TABLE_ENTRY,
						                      [](CatalogEntry &) { throw FoundTempObject {}; });
					}
				} catch (FoundTempObject &) {
					has_temp_objects = true;
				}
				if (has_temp_objects) {
					schema_list.emplace_back("pg_temp");
				}
			}
			schema_list.emplace_back("pg_catalog");
		}
		// PG-compliant: only include user-set schemas that actually exist,
		// with "$user" resolved via the current session user.
		for (auto &entry : catalog_search_path->GetResolvedSetPaths()) {
			if (entry.catalog != current_catalog) {
				continue;
			}
			auto schema_entry =
			    Catalog::GetSchema(context, entry.catalog, entry.schema, OnEntryNotFound::RETURN_NULL);
			if (!schema_entry) {
				continue;
			}
			schema_list.emplace_back(entry.schema);
		}
		result_val = Value::LIST(LogicalType::VARCHAR, schema_list);
	}
	return make_uniq<CurrentSchemasBindData>(std::move(result_val));
}

// current_schemas
void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<CurrentSchemasBindData>();
	result.Reference(info.result);
}

// in_search_path
void InSearchPathFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &search_path = ClientData::Get(context).catalog_search_path;
	BinaryExecutor::Execute<string_t, string_t, bool>(
	    input.data[0], input.data[1], result, input.size(), [&](string_t db_name, string_t schema_name) {
		    return search_path->SchemaInSearchPath(context, db_name.GetString(), schema_name.GetString());
	    });
}

// txid_current
void TransactionIdCurrent(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &catalog = Catalog::GetCatalog(context, DatabaseManager::GetDefaultDatabase(context));
	auto &transaction = DuckTransaction::Get(context, catalog);
	auto val = Value::UBIGINT(transaction.start_time);
	result.Reference(val);
}

// version
void VersionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto val = Value(DuckDB::LibraryVersion());
	result.Reference(val);
}

} // namespace

ScalarFunction CurrentQueryFun::GetFunction() {
	ScalarFunction current_query({}, LogicalType::VARCHAR, CurrentQueryFunction);
	current_query.SetStability(FunctionStability::VOLATILE);
	return current_query;
}

ScalarFunction CurrentSchemaFun::GetFunction() {
	ScalarFunction current_schema({}, LogicalType::VARCHAR, CurrentSchemaFunction);
	current_schema.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return current_schema;
}

ScalarFunction CurrentDatabaseFun::GetFunction() {
	ScalarFunction current_database({}, LogicalType::VARCHAR, CurrentDatabaseFunction);
	current_database.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return current_database;
}

ScalarFunction CurrentSchemasFun::GetFunction() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunction current_schemas({LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction,
	                               CurrentSchemasBind);
	current_schemas.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return current_schemas;
}

ScalarFunction InSearchPathFun::GetFunction() {
	ScalarFunction in_search_path({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                              InSearchPathFunction);
	in_search_path.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return in_search_path;
}

ScalarFunction CurrentTransactionIdFun::GetFunction() {
	ScalarFunction txid_current({}, LogicalType::UBIGINT, TransactionIdCurrent);
	txid_current.SetStability(FunctionStability::CONSISTENT_WITHIN_QUERY);
	return txid_current;
}

ScalarFunction VersionFun::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, VersionFunction);
}

} // namespace duckdb
