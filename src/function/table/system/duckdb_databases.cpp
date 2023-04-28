#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct DuckDBDatabasesData : public GlobalTableFunctionState {
	DuckDBDatabasesData() : offset(0) {
	}

	vector<reference<AttachedDatabase>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBDatabasesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBDatabasesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBDatabasesData>();

	// scan all the schemas for tables and collect them and collect them
	auto &db_manager = DatabaseManager::Get(context);
	result->entries = db_manager.GetDatabases(context);
	return std::move(result);
}

void DuckDBDatabasesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBDatabasesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];

		auto &attached = entry.get().Cast<AttachedDatabase>();
		// return values:

		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, count, attached.GetName());
		// database_oid, BIGINT
		output.SetValue(col++, count, Value::BIGINT(attached.oid));
		// path, VARCHAR
		bool is_internal = attached.IsSystem() || attached.IsTemporary();
		Value db_path;
		if (!is_internal) {
			bool in_memory = attached.GetCatalog().InMemory();
			if (!in_memory) {
				db_path = Value(attached.GetCatalog().GetDBPath());
			}
		}
		output.SetValue(col++, count, db_path);
		// internal, BOOLEAN
		output.SetValue(col++, count, Value::BOOLEAN(is_internal));
		// type, VARCHAR
		output.SetValue(col++, count, Value(attached.GetCatalog().GetCatalogType()));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBDatabasesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_databases", {}, DuckDBDatabasesFunction, DuckDBDatabasesBind, DuckDBDatabasesInit));
}

} // namespace duckdb
